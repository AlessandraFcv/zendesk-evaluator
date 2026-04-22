require('dotenv').config();
const express    = require('express');
const bcrypt     = require('bcryptjs');
const jwt        = require('jsonwebtoken');
const cors       = require('cors');
const https      = require('https');
const nodemailer = require('nodemailer');
const { google } = require('googleapis');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const { v4: uuidv4 } = require('uuid');
const fs   = require('fs');
const path = require('path');
const db   = require('./db');

/* ══════════════════════════════════════════════════════════════
   MODIFICAÇÕES:
   - Adicionada função zendeskChatRequest() para API do Zendesk Chat
   - Rota /api/admin/sync-agents modificada para:
     * Buscar agentes de /api/v2/chat/agents
     * Usar departamentos (enabled_departments) como grupos/segmentos
     * Mapear agent.departments ao invés de group_memberships
══════════════════════════════════════════════════════════════ */

const app        = express();
const PORT       = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || 'zendesk_evaluator_secret_2024';

app.use(cors());
app.use(express.json({ limit: '2mb' }));

const PUBLIC_DIR = path.join(__dirname, 'public');
app.use(express.static(PUBLIC_DIR));
app.get('/', (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'index.html')));

/* ══════════════════════════════════════════════════════════════
   HELPERS DE BANCO
══════════════════════════════════════════════════════════════ */

async function getCfg() {
  const { rows } = await db.query('SELECT * FROM configuracoes LIMIT 1');
  return rows[0] || {};
}

const CONFIG_EDITABLE_FIELDS = [
  'zendesk_subdominio',
  'zendesk_email',
  'zendesk_token',
  'smtp_servidor',
  'smtp_porta',
  'smtp_seguro',
  'smtp_usuario',
  'smtp_senha',
  'smtp_nome_remetente',
  'ia_chave_api',
  'ia_modelo',
  'documentacao_base',
];

function isScopedCfgValueSet(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'string') return v.trim() !== '';
  return true;
}

function mergeCfg(baseCfg, scopedCfg) {
  const merged = { ...(baseCfg || {}) };
  if (!scopedCfg) return merged;
  CONFIG_EDITABLE_FIELDS.forEach((field) => {
    if (isScopedCfgValueSet(scopedCfg[field])) {
      merged[field] = scopedCfg[field];
    }
  });
  return merged;
}

async function getScopedCfgByUserId(userId) {
  if (!userId) return null;
  try {
    const { rows } = await db.query(
      'SELECT * FROM configuracoes_responsavel WHERE usuario_id = $1::uuid LIMIT 1',
      [userId]
    );
    return rows[0] || null;
  } catch (e) {
    // Mantem compatibilidade em ambientes sem migração aplicada.
    if (/configuracoes_responsavel/i.test(String(e?.message || ''))) return null;
    throw e;
  }
}

async function getCfgForUser(user) {
  const globalCfg = await getCfg();
  if (!user || user.role !== 'manager') return globalCfg;
  const scopedCfg = await getScopedCfgByUserId(user.id);
  return mergeCfg(globalCfg, scopedCfg);
}

function cfgToApiDto(cfg, extras = {}) {
  const c = cfg || {};
  return {
    zendeskSubdomain: c.zendesk_subdominio || '',
    zendeskEmail: c.zendesk_email || '',
    zendeskHasToken: !!c.zendesk_token,
    smtpHost: c.smtp_servidor || '',
    smtpPort: c.smtp_porta || 587,
    smtpSecure: c.smtp_seguro || false,
    smtpUser: c.smtp_usuario || '',
    smtpHasPass: !!c.smtp_senha,
    smtpFromName: c.smtp_nome_remetente || 'TecnoIT',
    anthropicHasKey: !!c.ia_chave_api,
    aiModel: c.ia_modelo || 'gemini-2.0-flash',
    basicampDocs: c.documentacao_base || '',
    ...extras,
  };
}

// Mapa assignee_id_zendesk → nome_consultor
async function getAssigneeMap() {
  const { rows } = await db.query(`
    SELECT ia.id_zendesk, u.nome
    FROM ids_atendente ia JOIN usuarios u ON u.id = ia.usuario_id
  `);
  const map = {};
  rows.forEach(r => { map[r.id_zendesk] = r.nome; });
  return map;
}

// Mapa requester_id_zendesk (usuarios.id_zendesk) → dados do consultor
async function getRequesterConsultantMap() {
  const { rows } = await db.query(`
    SELECT id, id_zendesk, nome
    FROM usuarios
    WHERE id_zendesk IS NOT NULL
  `);
  const map = {};
  rows.forEach(r => {
    map[String(r.id_zendesk)] = { user_id: r.id, name: r.nome };
  });
  return map;
}

// Retorna assignee_ids Zendesk dos grupos gerenciados por userId
async function getManagerAgentIds(userId) {
  const grupoIds = await getManagedActualGroupIds(userId);
  if (!grupoIds.length) return null;
  const { rows } = await db.query(`
    SELECT ia.id_zendesk
    FROM ids_atendente ia
    JOIN membros_grupo mg ON mg.usuario_id = ia.usuario_id
    WHERE mg.grupo_id = ANY($1::uuid[])
  `, [grupoIds]);
  return rows.map(r => r.id_zendesk);
}

// Retorna requester_ids Zendesk (usuarios.id_zendesk) dos grupos gerenciados por userId
async function getManagerRequesterIds(userId) {
  const grupoIds = await getManagedActualGroupIds(userId);
  if (!grupoIds.length) return [];
  const { rows } = await db.query(`
    SELECT DISTINCT u.id_zendesk
    FROM usuarios u
    JOIN membros_grupo mg ON mg.usuario_id = u.id
    WHERE mg.grupo_id = ANY($1::uuid[])
      AND u.id_zendesk IS NOT NULL
  `, [grupoIds]);
  return rows.map(r => String(r.id_zendesk));
}

/* ══════════════════════════════════════════════════════════════
   MIDDLEWARES
══════════════════════════════════════════════════════════════ */

function authenticate(req, res, next) {
  const h = req.headers.authorization;
  if (!h || !h.startsWith('Bearer ')) return res.status(401).json({ error: 'Token nao fornecido' });
  try { req.user = jwt.verify(h.split(' ')[1], JWT_SECRET); next(); }
  catch { return res.status(401).json({ error: 'Token invalido' }); }
}

async function canEvaluate(req, res, next) {
  if (req.user.role === 'admin' || req.user.role === 'evaluator') return next();
  return res.status(403).json({ error: 'Acesso restrito a avaliadores' });
}

function adminOnly(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Acesso restrito ao administrador' });
  next();
}

function adminOrManagerOnly(req, res, next) {
  if (req.user.role !== 'admin' && req.user.role !== 'manager') {
    return res.status(403).json({ error: 'Acesso restrito a administradores e responsaveis' });
  }
  next();
}

async function canManage(req, res, next) {
  if (req.user.role === 'admin' || req.user.role === 'evaluator' || req.user.role === 'manager') return next();
  return res.status(403).json({ error: 'Acesso restrito' });
}

/* ══════════════════════════════════════════════════════════════
   ZENDESK + IA
══════════════════════════════════════════════════════════════ */

const ZENDESK_MAX_REQUESTS_PER_MINUTE = 300;
const ZENDESK_RATE_WINDOW_MS = 60 * 1000;
const ZENDESK_RATE_WAIT_MS = 20 * 1000;
const zendeskRequestTimestamps = [];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForZendeskRateSlot() {
  while (true) {
    const now = Date.now();
    while (
      zendeskRequestTimestamps.length &&
      (now - zendeskRequestTimestamps[0]) >= ZENDESK_RATE_WINDOW_MS
    ) {
      zendeskRequestTimestamps.shift();
    }
    if (zendeskRequestTimestamps.length < ZENDESK_MAX_REQUESTS_PER_MINUTE) {
      zendeskRequestTimestamps.push(now);
      return;
    }
    console.warn('[ZENDESK RATE LIMIT] Limite local de 300 req/min atingido. Aguardando 20s para continuar...');
    await sleep(ZENDESK_RATE_WAIT_MS);
  }
}

function buildZendeskRateLimitError(headers) {
  const err = new Error('Number of allowed API requests per minute exceeded');
  err.code = 'ZENDESK_RATE_LIMIT';
  const retryAfterSeconds = Number(headers?.['retry-after']);
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    err.retryAfterMs = retryAfterSeconds * 1000;
  }
  return err;
}

async function runZendeskThrottled(requestFn) {
  while (true) {
    await waitForZendeskRateSlot();
    try {
      return await requestFn();
    } catch (e) {
      if (e?.code === 'ZENDESK_RATE_LIMIT') {
        const waitMs = Math.max(ZENDESK_RATE_WAIT_MS, Number(e.retryAfterMs || 0));
        console.warn(`[ZENDESK RATE LIMIT] Zendesk retornou 429. Aguardando ${Math.ceil(waitMs / 1000)}s e continuando...`);
        await sleep(waitMs);
        continue;
      }
      throw e;
    }
  }
}

function logZendeskConsult(urlPath, context = '') {
  try {
    const u = new URL(urlPath, 'https://local.zendesk.com');
    const pathname = String(u.pathname || '');
    const isTracked =
      pathname.includes('/api/v2/search.json')
      || pathname.includes('/api/v2/satisfaction_ratings.json')
      || pathname.includes('/api/v2/tickets.json');
    if (!isTracked) return;

    const prefix = context ? `[ZENDESK CONSULTA][${context}]` : '[ZENDESK CONSULTA]';
    const query = u.searchParams.get('query');
    const params = {};
    u.searchParams.forEach((v, k) => {
      if (k === 'query') return;
      params[k] = v;
    });

    if (query) {
      console.log(`${prefix} ${pathname} query=${query} params=${JSON.stringify(params)}`);
    } else {
      console.log(`${prefix} ${pathname} params=${JSON.stringify(params)}`);
    }
  } catch (_) {
    const prefix = context ? `[ZENDESK CONSULTA][${context}]` : '[ZENDESK CONSULTA]';
    console.log(`${prefix} ${String(urlPath || '')}`);
  }
}

function zendeskRequest(urlPath, cfg, context = '') {
  logZendeskConsult(urlPath, context);
  return runZendeskThrottled(() => new Promise((resolve, reject) => {
    const auth = Buffer.from(cfg.zendesk_email + '/token:' + cfg.zendesk_token).toString('base64');
    const url  = 'https://' + cfg.zendesk_subdominio + '.zendesk.com' + urlPath;
    const req  = https.request(url, { method:'GET', headers:{ 'Authorization':'Basic '+auth, 'Content-Type':'application/json' } }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (Number(res.statusCode) === 429) return reject(buildZendeskRateLimitError(res.headers));
        try {
          const p = d ? JSON.parse(d) : {};
          if (res.statusCode !== 200) return reject(new Error(p.error || p.description || 'HTTP ' + res.statusCode));
          resolve(p);
        } catch (e) { reject(new Error('Resposta invalida do Zendesk (HTTP '+res.statusCode+'): ' + d.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.end();
  }));
}

function zendeskChatRequest(urlPath, cfg) {
  return runZendeskThrottled(() => new Promise((resolve, reject) => {
    const auth = Buffer.from(cfg.zendesk_email + '/token:' + cfg.zendesk_token).toString('base64');
    const url  = 'https://' + cfg.zendesk_subdominio + '.zendesk.com' + urlPath;
    const req  = https.request(url, { method:'GET', headers:{ 'Authorization':'Basic '+auth, 'Content-Type':'application/json' } }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (Number(res.statusCode) === 429) return reject(buildZendeskRateLimitError(res.headers));
        try {
          const p = d ? JSON.parse(d) : {};
          if (res.statusCode !== 200) return reject(new Error(p.error || p.description || 'HTTP ' + res.statusCode));
          resolve(p);
        } catch (e) { reject(new Error('Resposta invalida do Zendesk Chat (HTTP '+res.statusCode+'): ' + d.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.end();
  }));
}

function getZendeskNextPath(nextPageUrl) {
  if (!nextPageUrl) return null;
  try {
    const u = new URL(nextPageUrl);
    return `${u.pathname}${u.search}`;
  } catch (_) {
    return null;
  }
}

const ALLOWED_USER_TYPES = ['coordenador', 'lider_tecnico', 'n2', 'n1'];

function normalizeUserType(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, '_');
  if (!normalized) return null;
  return ALLOWED_USER_TYPES.includes(normalized) ? normalized : null;
}

function isN1Consultant(user) {
  return String(user?.user_type || '').trim() === 'n1';
}

function filterN1Consultants(list) {
  return (Array.isArray(list) ? list : []).filter(isN1Consultant);
}

async function fetchZendeskSearchResults(urlPath, cfg, context = '', maxPages = 60) {
  const results = [];
  let nextPath = urlPath;
  let guard = 0;

  while (nextPath && guard < maxPages) {
    guard += 1;
    const pageData = await zendeskRequest(nextPath, cfg, context);
    if (Array.isArray(pageData?.results)) {
      results.push(...pageData.results);
    }
    nextPath = getZendeskNextPath(pageData?.next_page);
  }

  return results;
}

function isZendeskAgentEnabled(agent) {
  if (!agent || typeof agent !== 'object') return false;
  if (agent.enabled === false) return false;
  if (agent.active === false) return false;
  if (agent.suspended === true) return false;
  return true;
}

function extractZendeskAgentGroupIds(agent) {
  if (!agent || typeof agent !== 'object') return [];
  const raw = agent.enabled_departments || agent.departments || agent.group_ids || [];
  if (!Array.isArray(raw)) return [];
  return raw
    .map((g) => {
      if (g === null || g === undefined) return '';
      if (typeof g === 'object') return String(g.id || g.group_id || '').trim();
      return String(g).trim();
    })
    .filter(Boolean);
}

async function fetchZendeskSupportGroups(cfg) {
  const groups = [];
  let path = '/api/v2/groups.json?per_page=100';
  let guard = 0;
  while (path && guard < 50) {
    guard++;
    const pageData = await zendeskRequest(path, cfg, 'SYNC_GROUPS_SUPPORT');
    groups.push(...(pageData.groups || []));
    path = getZendeskNextPath(pageData.next_page);
  }
  return groups;
}

async function fetchZendeskSupportGroupMembershipMap(cfg) {
  const map = {};
  let path = '/api/v2/group_memberships.json?per_page=100';
  let guard = 0;
  while (path && guard < 100) {
    guard++;
    const pageData = await zendeskRequest(path, cfg, 'SYNC_GROUP_MEMBERSHIPS');
    (pageData.group_memberships || []).forEach((gm) => {
      const uid = String(gm?.user_id || '').trim();
      const gid = String(gm?.group_id || '').trim();
      if (!uid || !gid) return;
      if (!map[uid]) map[uid] = [];
      if (!map[uid].includes(gid)) map[uid].push(gid);
    });
    path = getZendeskNextPath(pageData.next_page);
  }
  return map;
}

async function fetchZendeskSupportAgents(cfg) {
  const users = [];
  const seen = new Set();
  let path = '/api/v2/users.json?role[]=agent&role[]=admin&per_page=100';
  let guard = 0;
  while (path && guard < 50) {
    guard++;
    const pageData = await zendeskRequest(path, cfg, 'SYNC_AGENTS_SUPPORT');
    (pageData.users || []).forEach((u) => {
      const id = String(u?.id || '').trim();
      if (!id || seen.has(id)) return;
      seen.add(id);
      users.push(u);
    });
    path = getZendeskNextPath(pageData.next_page);
  }

  if (!users.length) {
    let searchPath = `/api/v2/search.json?query=${encodeURIComponent('type:user role:agent')}&per_page=100`;
    let searchGuard = 0;
    while (searchPath && searchGuard < 30) {
      searchGuard++;
      const pageData = await zendeskRequest(searchPath, cfg, 'SYNC_AGENTS_SEARCH');
      (pageData.results || []).forEach((u) => {
        const id = String(u?.id || '').trim();
        if (!id || seen.has(id)) return;
        seen.add(id);
        users.push(u);
      });
      searchPath = getZendeskNextPath(pageData.next_page);
    }
  }

  return users.filter((u) => {
    const role = String(u?.role || '').toLowerCase();
    return role === 'agent' || role === 'admin';
  });
}

function zendeskPut(urlPath, body, cfg) {
  return runZendeskThrottled(() => new Promise((resolve, reject) => {
    const auth    = Buffer.from(cfg.zendesk_email + '/token:' + cfg.zendesk_token).toString('base64');
    const url     = 'https://' + cfg.zendesk_subdominio + '.zendesk.com' + urlPath;
    const payload = JSON.stringify(body);
    const req     = https.request(url, {
      method: 'PUT',
      headers: { 'Authorization': 'Basic ' + auth, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (Number(res.statusCode) === 429) return reject(buildZendeskRateLimitError(res.headers));
        try {
          const p = d ? JSON.parse(d) : {};
          if (res.statusCode < 200 || res.statusCode > 299) return reject(new Error(p.error || p.description || 'HTTP ' + res.statusCode));
          resolve(p);
        } catch (e) { reject(new Error('Resposta invalida do Zendesk PUT: ' + d.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  }));
}

// Busca artigos no Help Center do Zendesk (endpoint público)
async function searchHelpCenterArticles(category, query, cfg) {
  return runZendeskThrottled(() => new Promise((resolve, reject) => {
    try {
      const encodedCategory = encodeURIComponent(category);
      const encodedQuery = encodeURIComponent(query);
      const urlPath = `/api/v2/help_center/articles/search.json?category=${encodedCategory}&query=${encodedQuery}`;
      const fullUrl = `https://${cfg.zendesk_subdominio}.zendesk.com${urlPath}`;
      
      const req = https.request(fullUrl, { 
        method: 'GET', 
        headers: { 'Content-Type': 'application/json' } 
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          if (Number(res.statusCode) === 429) return reject(buildZendeskRateLimitError(res.headers));
          try {
            const parsed = JSON.parse(data);
            if (res.statusCode !== 200) {
              console.warn('[HELP CENTER SEARCH] HTTP', res.statusCode, parsed);
              return resolve([]);
            }
            const artigos = (parsed.results || []).map(article => ({
              title: article.title,
              url: article.html_url,
              snippet: article.snippet
            }));
            resolve(artigos);
          } catch (e) {
            console.warn('[HELP CENTER SEARCH] Parse error:', e.message);
            resolve([]);
          }
        });
      });
      
      req.on('error', (e) => {
        console.warn('[HELP CENTER SEARCH] Request error:', e.message);
        resolve([]);
      });
      
      req.end();
    } catch (e) {
      console.warn('[HELP CENTER SEARCH] Error:', e.message);
      resolve([]);
    }
  }));
}

// Detecta rejeições mencionadas na conversa
function extractRejections(conversa) {
  const rejectionPattern = /rejei[çc][ãa]o[:\s]+([^\n.]+)/gi;
  const matches = [];
  let match;
  while ((match = rejectionPattern.exec(conversa)) !== null) {
    const rejection = match[1].trim();
    if (rejection.length > 5 && rejection.length < 200) {
      matches.push(rejection);
    }
  }
  return [...new Set(matches)]; // Remove duplicatas
}

async function callGemini(system, user, cfg) {
  if (!cfg.ia_chave_api) throw new Error('Chave Gemini nao configurada em Configuracoes.');
  const genAI  = new GoogleGenerativeAI(cfg.ia_chave_api);
  const model  = genAI.getGenerativeModel({
    model: cfg.ia_modelo || 'gemini-2.0-flash',
    systemInstruction: system,
  });
  const result   = await model.generateContent(user);
  const response = await result.response;
  return response.text() || '';
}

function extractJSON(raw) {
  // Remove blocos markdown ```json ... ``` ou ``` ... ```
  let s = raw.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
  // Extrai o primeiro objeto JSON encontrado na string
  const start = s.indexOf('{');
  const end   = s.lastIndexOf('}');
  if (start === -1 || end === -1) throw new Error('Nenhum JSON encontrado na resposta da IA');
  return JSON.parse(s.slice(start, end + 1));
}

async function sendMail(opts, cfg) {
  if (!cfg.smtp_servidor || !cfg.smtp_usuario) throw new Error('SMTP nao configurado');
  const t = nodemailer.createTransport({ host:cfg.smtp_servidor, port:Number(cfg.smtp_porta)||587, secure:!!cfg.smtp_seguro, auth:{user:cfg.smtp_usuario,pass:cfg.smtp_senha} });
  await t.sendMail({ from:'"'+(cfg.smtp_nome_remetente||'TecnoIT')+'" <'+cfg.smtp_usuario+'>', ...opts });
}

async function buildTicketRow(zid, assigneeId, t, payload, tipo = 'normal', amap = null) {
  const assigneeMap = amap || await getAssigneeMap();
  // Resolve consultor_id pelo assignee_id
  const { rows } = await db.query(
    'SELECT usuario_id FROM ids_atendente WHERE id_zendesk = $1 LIMIT 1',
    [String(assigneeId)]
  );
  return {
    id_zendesk:           zid,
    tipo,
    assunto:              t.subject        || payload.subject        || 'Sem assunto',
    descricao:            t.description    || payload.description    || '',
    status:               t.status         || 'resolvido',
    canal:                t.channel        || t.via?.channel         || 'web',
    consultor_id:         rows[0]?.usuario_id || null,
    id_assignee_zendesk:  String(assigneeId),
    nome_consultor:       assigneeMap[String(assigneeId)] || 'ID: '+assigneeId,
    nome_cliente:         t.requester?.name  || t.requester_name  || payload.requester_name  || 'Cliente',
    email_cliente:        t.requester?.email || t.requester_email || payload.requester_email || null,
    tags:                 t.tags || [],
    iniciado_por_ia:      (t.tags || []).includes('claudia_escalado_n2'),
    criado_no_zendesk:    t.created_at || null,
    resolvido_no_zendesk: t.updated_at || t.solved_at || null,
  };
}

function toPlainText(v) {
  return String(v || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/\r\n/g, '\n')
    .replace(/\u00a0/g, ' ')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function extractPdcaScore(text) {
  const src = String(text || '');
  const m = /nota\s*[:\-]\s*([0-9]{1,2}(?:[.,][0-9]{1,2})?)/i.exec(src);
  if (!m) return null;
  const score = Number(String(m[1]).replace(',', '.'));
  if (!Number.isFinite(score)) return null;
  return Number(score.toFixed(2));
}

function mergeDescriptions(baseDescription, interactionsText) {
  const base = toPlainText(baseDescription);
  const interactions = toPlainText(interactionsText);
  if (!base && !interactions) return '';
  if (!interactions) return base;
  if (!base) return interactions;
  if (interactions.includes(base)) return interactions;
  return `${base}\n\n--- Interacoes do Ticket ---\n${interactions}`;
}

async function fetchZendeskTicketInteractions(cfg, ticketId) {
  let urlPath = `/api/v2/tickets/${encodeURIComponent(String(ticketId))}/comments.json?per_page=100&sort_order=asc`;
  const chunks = [];
  let guard = 0;
  while (urlPath && guard < 15) {
    guard++;
    const data = await zendeskRequest(urlPath, cfg);
    (data.comments || []).forEach(c => {
      const author = c?.author?.name || c?.via?.source?.from?.name || 'Autor';
      const body = toPlainText(c?.plain_body || c?.body || '');
      if (!body) return;
      chunks.push(`[${author}] ${body}`);
    });
    if (!data.next_page) break;
    try {
      const u = new URL(data.next_page);
      urlPath = `${u.pathname}${u.search}`;
    } catch (_) {
      urlPath = null;
    }
  }
  return chunks.join('\n\n');
}

/* ══════════════════════════════════════════════════════════════
   AUTH
══════════════════════════════════════════════════════════════ */

function parseMonthPeriod(monthRef) {
  const raw = String(monthRef || '').trim();
  const m = /^(\d{4})-(\d{2})$/.exec(raw);
  if (!m) throw new Error('Mes invalido. Use o formato YYYY-MM');
  const year = Number(m[1]);
  const month = Number(m[2]);
  if (!year || month < 1 || month > 12) throw new Error('Mes invalido. Use o formato YYYY-MM');
  const startUtc = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
  const endUtc = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
  return { month: `${year}-${String(month).padStart(2, '0')}`, startUtc, endUtc };
}

function parseRollingDaysPeriod(days) {
  const totalDays = Math.max(1, Number(days || 1));
  const endUtc = new Date();
  const startUtc = new Date(endUtc.getTime() - (totalDays * 24 * 60 * 60 * 1000));
  return { days: totalDays, startUtc, endUtc };
}

function parseCsvQueryIds(raw) {
  if (raw === undefined || raw === null) return [];
  return [...new Set(String(raw)
    .split(',')
    .map(s => s.trim())
    .filter(Boolean))];
}

function round2(v) {
  return Number(Number(v).toFixed(2));
}

const HISTORY_SOURCE_MAX_LEN = 30;

function normalizeHistorySourceTag(value, fallback = 'database') {
  const raw = String(value || fallback || 'database').trim() || String(fallback || 'database').trim() || 'database';
  const aliases = {
    google_sheets_prev_month_fallback: 'google_sheets_prev_fallback',
  };
  const normalized = aliases[raw] || raw;
  return normalized.length > HISTORY_SOURCE_MAX_LEN
    ? normalized.slice(0, HISTORY_SOURCE_MAX_LEN)
    : normalized;
}

function calcTotalAtendimentosNote(totalEquipe, totalConsultores, totalConsultor) {
  const teamTotal = Number(totalEquipe || 0);
  const consultantCount = Math.max(0, Number(totalConsultores || 0));
  const consultantTotal = Number(totalConsultor || 0);
  if (!consultantCount) return 0;
  const teamAverage = teamTotal / consultantCount;
  if (!Number.isFinite(teamAverage) || teamAverage <= 0) return 0;

  const indice = consultantTotal / teamAverage;
  let nota = 0;
  if (indice <= 1) nota = 8 * indice;
  else if (indice >= 1.2) nota = 10;
  else nota = 8 + 10 * (indice - 1);

  return round2(Math.max(0, Math.min(10, nota)));
}

const GOOGLE_PEER_REVIEW_SPREADSHEET_ID = process.env.GOOGLE_PEER_REVIEW_SPREADSHEET_ID || '1efsY4SaFUoCOCVyUlL4FUvHHQXQw-8A7RNyqA4uQfwM';
const GOOGLE_PEER_REVIEW_SHEET_GID = String(process.env.GOOGLE_PEER_REVIEW_SHEET_GID || '1443887643');
const GOOGLE_PEER_REVIEW_RANGE = process.env.GOOGLE_PEER_REVIEW_RANGE || 'A:ZZ';
const GOOGLE_PEER_REVIEW_CREDENTIALS_FILE = process.env.GOOGLE_PEER_REVIEW_CREDENTIALS_FILE
  || path.resolve(__dirname, '..', 'credenciais para conectar com o google.txt');

const CONSULTANT_RANK_METRICS = ['csat', 'atendimento', 'negativados', 'pdca', 'total_atendimentos', 'avaliacoes_pares'];

function normalizeText(v) {
  return String(v || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function normalizeEmail(v) {
  return String(v || '').trim().toLowerCase();
}

function toLoginSlug(v) {
  return normalizeText(v)
    .replace(/[^a-z0-9.\s_-]/g, '')
    .replace(/[ _-]+/g, '.')
    .replace(/\.+/g, '.')
    .replace(/^\.|\.$/g, '');
}

function dedupeStringIds(values) {
  return [...new Set((Array.isArray(values) ? values : [])
    .map(v => String(v || '').trim())
    .filter(Boolean))];
}

async function getManagedGroupIdsByUserId(userId) {
  if (!userId) return [];
  const { rows } = await db.query(
    'SELECT DISTINCT grupo_id::text AS id FROM responsaveis_grupo WHERE usuario_id = $1::uuid',
    [userId]
  );
  return rows.map(r => String(r.id));
}

async function getManagerOperableGroupIds(userId) {
  const managedIds = await getManagedGroupIdsByUserId(userId);
  if (!managedIds.length) return [];
  const actualIds = await resolveEffectiveGroupIdsToActualGroupIds(managedIds);
  return dedupeStringIds([...managedIds, ...actualIds]);
}

async function managerCanManageGroup(userId, groupId) {
  if (!userId || !groupId) return false;
  const allowedIds = new Set(await getManagerOperableGroupIds(userId));
  return allowedIds.has(String(groupId));
}

async function managerCanManageUser(userId, targetUserId) {
  if (!userId || !targetUserId) return false;
  if (String(userId) === String(targetUserId)) return true;
  const operableGroupIds = await getManagedActualGroupIds(userId);
  if (!operableGroupIds.length) return false;
  const { rows } = await db.query(`
    SELECT 1
    FROM membros_grupo mg
    WHERE mg.grupo_id = ANY($1::uuid[])
      AND mg.usuario_id = $2::uuid
    LIMIT 1
  `, [operableGroupIds, targetUserId]);
  return !!rows.length;
}

async function ensureManageableGroupIds(reqUser, groupIds) {
  const ids = dedupeStringIds(groupIds);
  if (!ids.length || reqUser.role === 'admin') return ids;
  const allowed = new Set(await getManagerOperableGroupIds(reqUser.id));
  const invalid = ids.find(id => !allowed.has(String(id)));
  if (invalid) {
    const err = new Error('Grupo fora do escopo do responsavel');
    err.statusCode = 403;
    throw err;
  }
  return ids;
}

async function ensureManageableGroup(reqUser, groupId) {
  const id = String(groupId || '').trim();
  if (!id) {
    const err = new Error('groupId obrigatorio');
    err.statusCode = 400;
    throw err;
  }
  if (reqUser.role === 'admin') return id;
  const ok = await managerCanManageGroup(reqUser.id, id);
  if (!ok) {
    const err = new Error('Grupo fora do escopo do responsavel');
    err.statusCode = 403;
    throw err;
  }
  return id;
}

async function ensureOwnedManagedGroup(reqUser, groupId) {
  const id = String(groupId || '').trim();
  if (!id) {
    const err = new Error('groupId obrigatorio');
    err.statusCode = 400;
    throw err;
  }
  if (reqUser.role === 'admin') return id;
  const managedIds = new Set(await getManagedGroupIdsByUserId(reqUser.id));
  if (!managedIds.has(id)) {
    const err = new Error('Grupo fora do escopo do responsavel');
    err.statusCode = 403;
    throw err;
  }
  return id;
}

async function ensureManageableUser(reqUser, userId) {
  const targetId = String(userId || '').trim();
  if (!targetId) {
    const err = new Error('user_id obrigatorio');
    err.statusCode = 400;
    throw err;
  }
  if (reqUser.role === 'admin') return targetId;
  const ok = await managerCanManageUser(reqUser.id, targetId);
  if (!ok) {
    const err = new Error('Usuario fora do escopo do responsavel');
    err.statusCode = 403;
    throw err;
  }
  return targetId;
}

async function getNextAvailableLogin(seed, skipUserId = null) {
  const base = toLoginSlug(seed) || 'consultor';
  let candidate = base;
  let idx = 1;
  while (idx < 1000) {
    const params = skipUserId
      ? [candidate, skipUserId]
      : [candidate];
    const query = skipUserId
      ? 'SELECT id FROM usuarios WHERE LOWER(login) = LOWER($1) AND id <> $2::uuid LIMIT 1'
      : 'SELECT id FROM usuarios WHERE LOWER(login) = LOWER($1) LIMIT 1';
    const { rows } = await db.query(query, params);
    if (!rows.length) return candidate;
    idx += 1;
    candidate = `${base}.${idx}`;
  }
  return `${base}.${Date.now()}`;
}

function isAggregatorGroupRow(row) {
  const zid = String(row?.id_zendesk || '').trim().toLowerCase();
  return zid.startsWith('manual:') || (Array.isArray(row?.linked_group_ids) && row.linked_group_ids.length > 0);
}

async function fetchGroupDefinitions(groupIds = null) {
  const ids = dedupeStringIds(groupIds);
  const params = [];
  let whereSql = '';
  if (ids.length) {
    params.push(ids);
    whereSql = 'WHERE g.id = ANY($1::uuid[])';
  }

  const { rows } = await db.query(`
    SELECT
      g.id::text,
      g.id_zendesk,
      g.nome AS name,
      COALESCE(array_agg(DISTINCT gav.grupo_id::text) FILTER (WHERE gav.grupo_id IS NOT NULL), '{}') AS linked_group_ids,
      COALESCE(array_agg(DISTINCT child.nome) FILTER (WHERE child.nome IS NOT NULL), '{}') AS linked_group_names
    FROM grupos g
    LEFT JOIN grupo_agrupador_vinculos gav ON gav.agrupador_id = g.id
    LEFT JOIN grupos child ON child.id = gav.grupo_id
    ${whereSql}
    GROUP BY g.id, g.id_zendesk, g.nome
    ORDER BY g.nome
  `, params);

  return rows.map((row) => {
    const linkedGroupIds = dedupeStringIds(row.linked_group_ids || []);
    const linkedGroupNames = [...new Set((row.linked_group_names || []).map(v => String(v || '').trim()).filter(Boolean))];
    const kind = isAggregatorGroupRow({ ...row, linked_group_ids: linkedGroupIds }) ? 'aggregator' : 'zendesk';
    return {
      id: String(row.id),
      id_zendesk: row.id_zendesk ? String(row.id_zendesk) : null,
      name: String(row.name || '').trim(),
      linked_group_ids: linkedGroupIds,
      linked_group_names: linkedGroupNames,
      kind,
    };
  });
}

function resolveDefinitionActualGroupIds(definition, definitionMap) {
  if (!definition) return [];
  if (definition.kind !== 'aggregator') return [String(definition.id)];
  return dedupeStringIds((definition.linked_group_ids || []).filter((groupId) => {
    const child = definitionMap.get(String(groupId));
    return !!child && child.kind !== 'aggregator';
  }));
}

async function fetchConsultantsByActualGroupIds(groupIds) {
  const ids = dedupeStringIds(groupIds);
  if (!ids.length) return [];
  try {
    const { rows } = await db.query(`
      SELECT
        mg.grupo_id::text AS actual_group_id,
        u.id::text AS consultant_id,
        u.login,
        u.nome AS consultant_name,
        u.email,
        u.tipo_usuario AS user_type,
        u.foto_url AS photo_url,
        u.id_zendesk AS requester_id,
        COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids
      FROM membros_grupo mg
      JOIN usuarios u ON u.id = mg.usuario_id AND u.id_zendesk IS NOT NULL
      LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
      WHERE mg.grupo_id = ANY($1::uuid[])
      GROUP BY mg.grupo_id, u.id, u.login, u.nome, u.email, u.tipo_usuario, u.foto_url, u.id_zendesk
      ORDER BY u.nome
    `, [ids]);
    return rows;
  } catch (e) {
    if (String(e?.code) !== '42703') throw e;
    const { rows } = await db.query(`
      SELECT
        mg.grupo_id::text AS actual_group_id,
        u.id::text AS consultant_id,
        u.login,
        u.nome AS consultant_name,
        u.email,
        u.tipo_usuario AS user_type,
        NULL::text AS photo_url,
        u.id_zendesk AS requester_id,
        COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids
      FROM membros_grupo mg
      JOIN usuarios u ON u.id = mg.usuario_id AND u.id_zendesk IS NOT NULL
      LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
      WHERE mg.grupo_id = ANY($1::uuid[])
      GROUP BY mg.grupo_id, u.id, u.login, u.nome, u.email, u.tipo_usuario, u.id_zendesk
      ORDER BY u.nome
    `, [ids]);
    return rows;
  }
}

async function fetchActualGroupIdsByAssigneeIds(assigneeIds) {
  const ids = dedupeStringIds(assigneeIds);
  const groupsByAssignee = {};
  ids.forEach(id => { groupsByAssignee[id] = []; });
  if (!ids.length) return groupsByAssignee;
  const { rows } = await db.query(`
    SELECT DISTINCT
      mg.grupo_id::text AS group_id,
      u.id::text AS user_id,
      u.id_zendesk::text AS requester_id,
      ia.id_zendesk::text AS assignee_id
    FROM membros_grupo mg
    JOIN usuarios u ON u.id = mg.usuario_id
    LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
    WHERE u.id_zendesk::text = ANY($1::text[])
       OR u.id::text = ANY($1::text[])
       OR ia.id_zendesk::text = ANY($1::text[])
  `, [ids]);
  const requested = new Set(ids);
  rows.forEach((row) => {
    [row.user_id, row.requester_id, row.assignee_id].forEach((candidate) => {
      const key = String(candidate || '');
      if (!requested.has(key)) return;
      groupsByAssignee[key].push(row.group_id);
    });
  });
  Object.keys(groupsByAssignee).forEach((key) => {
    groupsByAssignee[key] = dedupeStringIds(groupsByAssignee[key]);
  });
  return groupsByAssignee;
}

async function resolveEffectiveGroupIdsToActualGroupIds(groupIds) {
  const ids = dedupeStringIds(groupIds);
  if (!ids.length) return [];
  const definitions = await fetchGroupDefinitions(ids);
  const foundIds = new Set(definitions.map(def => String(def.id)));
  const invalidGroup = ids.find(id => !foundIds.has(String(id)));
  if (invalidGroup) {
    const err = new Error('Grupo selecionado invalido');
    err.statusCode = 400;
    throw err;
  }
  const definitionMap = new Map(definitions.map(def => [String(def.id), def]));
  const linkedGroupIds = dedupeStringIds(definitions.flatMap(def => def.linked_group_ids || []));
  if (linkedGroupIds.length) {
    const childDefinitions = await fetchGroupDefinitions(linkedGroupIds);
    childDefinitions.forEach(def => definitionMap.set(String(def.id), def));
  }
  return dedupeStringIds(definitions.flatMap(def => resolveDefinitionActualGroupIds(def, definitionMap)));
}

async function getManagedActualGroupIds(userId) {
  const managedGroupIds = await getManagedGroupIdsByUserId(userId);
  return resolveEffectiveGroupIdsToActualGroupIds(managedGroupIds);
}

function monthRefFromDate(d) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

function getPreviousMonthRef(monthRef) {
  const period = parseMonthPeriod(monthRef);
  const prev = new Date(Date.UTC(
    period.startUtc.getUTCFullYear(),
    period.startUtc.getUTCMonth() - 1,
    1,
    0,
    0,
    0,
    0
  ));
  return `${prev.getUTCFullYear()}-${String(prev.getUTCMonth() + 1).padStart(2, '0')}`;
}

async function fetchPreviousMonthPeerReviewHistory(monthRef, consultants) {
  const consultantIds = (Array.isArray(consultants) ? consultants : [])
    .map(c => String(c?.id || '').trim())
    .filter(Boolean);
  if (!monthRef || !consultantIds.length) return [];

  const previousMonthRef = getPreviousMonthRef(monthRef);
  const { rows } = await db.query(`
    SELECT
      consultor_id::text AS consultant_id,
      avaliacoes_pares_percent,
      avaliacoes_pares_total,
      avaliacoes_pares_par_percent,
      avaliacoes_pares_par_total,
      avaliacoes_pares_gestor_percent,
      avaliacoes_pares_gestor_total,
      fonte_avaliacoes_pares,
      erro_avaliacoes_pares
    FROM historico_csat_consultor
    WHERE mes_ref = $1::date
      AND consultor_id = ANY($2::uuid[])
  `, [`${previousMonthRef}-01`, consultantIds]);
  return rows;
}

function applyPeerReviewsPreviousMonthFallback(card, fallbackRows, consultants, fallbackMonthRef) {
  const ranking = Array.isArray(card?.ranking) ? card.ranking : [];
  const consultantById = {};
  (Array.isArray(consultants) ? consultants : []).forEach(c => {
    consultantById[String(c.id)] = c;
  });

  const fallbackMap = {};
  (Array.isArray(fallbackRows) ? fallbackRows : []).forEach(row => {
    fallbackMap[String(row.consultant_id || '')] = row;
  });

  let usedFallback = false;
  const mergedRanking = ranking.map(row => {
    const cid = String(row.consultant_id || '');
    const hasCurrentScore = row.score !== null && row.score !== undefined;
    const hasCurrentTotals = Number(row.total || 0) > 0 || Number(row.par_total || 0) > 0 || Number(row.gestor_total || 0) > 0;
    if (hasCurrentScore || hasCurrentTotals) return row;

    const fallback = fallbackMap[cid];
    if (!fallback) return row;

    const fallbackScore = fallback.avaliacoes_pares_percent === null || fallback.avaliacoes_pares_percent === undefined
      ? null
      : round2(Number(fallback.avaliacoes_pares_percent));
    const fallbackTotal = Number(fallback.avaliacoes_pares_total || 0);
    const fallbackParScore = fallback.avaliacoes_pares_par_percent === null || fallback.avaliacoes_pares_par_percent === undefined
      ? null
      : round2(Number(fallback.avaliacoes_pares_par_percent));
    const fallbackParTotal = Number(fallback.avaliacoes_pares_par_total || 0);
    const fallbackGestorScore = fallback.avaliacoes_pares_gestor_percent === null || fallback.avaliacoes_pares_gestor_percent === undefined
      ? null
      : round2(Number(fallback.avaliacoes_pares_gestor_percent));
    const fallbackGestorTotal = Number(fallback.avaliacoes_pares_gestor_total || 0);

    const hasFallbackData = fallbackScore !== null || fallbackTotal > 0 || fallbackParScore !== null || fallbackParTotal > 0 || fallbackGestorScore !== null || fallbackGestorTotal > 0;
    if (!hasFallbackData) return row;

    usedFallback = true;
    return {
      consultant_id: row.consultant_id,
      consultant_name: row.consultant_name || consultantById[cid]?.name || `ID ${cid}`,
      score: fallbackScore,
      total: fallbackTotal,
      par_score: fallbackParScore,
      par_total: fallbackParTotal,
      gestor_score: fallbackGestorScore,
      gestor_total: fallbackGestorTotal,
      source: 'history_prev_month',
      error: null,
    };
  }).sort(sortRankingRows);

  const totals = mergedRanking.reduce((s, r) => {
    s.total += Number(r.total || 0);
    if (r.score !== null && Number(r.total || 0) > 0) {
      s.sum += Number(r.score) * Number(r.total || 0);
    }
    return s;
  }, { sum: 0, total: 0 });

  return {
    average: totals.total > 0 ? round2(totals.sum / totals.total) : null,
    total: totals.total,
    ranking: mergedRanking,
    source: usedFallback
      ? normalizeHistorySourceTag(card?.source === 'google_sheets' ? 'google_sheets_prev_month_fallback' : (card?.source || 'history_prev_month'), 'history_prev_month')
      : normalizeHistorySourceTag(card?.source || 'google_sheets', 'google_sheets'),
    error: card?.error || null,
    fallback_month: usedFallback ? fallbackMonthRef : null,
    used_fallback: usedFallback,
  };
}

const zendeskTicketFieldCache = new Map();

function prettifyZendeskTagValue(value) {
  return String(value || '')
    .replace(/^[a-z0-9]+[_:-]/i, '')
    .replace(/[_-]+/g, ' ')
    .trim();
}

async function fetchZendeskTicketFieldMap(cfg) {
  const cacheKey = String(cfg?.zendesk_subdominio || '').trim().toLowerCase();
  const cached = zendeskTicketFieldCache.get(cacheKey);
  const now = Date.now();
  if (cached && (now - cached.ts) < 10 * 60 * 1000) {
    return cached.map;
  }

  const fieldMap = {};
  let path = '/api/v2/ticket_fields.json?per_page=100';
  while (path) {
    const data = await zendeskRequest(path, cfg);
    (data.ticket_fields || []).forEach(field => {
      const id = String(field.id || '');
      if (!id) return;
      const label = [field.raw_title, field.title, field.key].filter(Boolean).join(' ');
      const options = {};
      (field.custom_field_options || []).forEach(opt => {
        const k = String(opt.value || '').trim();
        if (k) options[k] = String(opt.name || opt.value || '').trim();
      });
      fieldMap[id] = {
        name: normalizeText(label),
        options,
      };
    });

    if (!data.next_page) break;
    try {
      const u = new URL(data.next_page);
      path = u.pathname + u.search;
    } catch (_) {
      path = null;
    }
  }

  zendeskTicketFieldCache.set(cacheKey, { ts: now, map: fieldMap });
  return fieldMap;
}

function extractImportTicketMeta(ticket, ticketFieldMap) {
  const result = { categoria: null, documento: null, produto: null };
  const normalizedMap = {
    categoria: ['categoria', 'category'],
    documento: ['documento', 'document', 'doc'],
    produto: ['produto', 'product'],
  };

  const setValue = (key, raw, options = null) => {
    if (result[key]) return;
    if (raw === undefined || raw === null || raw === '') return;
    const arr = Array.isArray(raw) ? raw : [raw];
    const parts = arr.map(v => {
      const value = String(v || '').trim();
      if (!value) return '';
      if (options && options[value]) return String(options[value]).trim();
      return prettifyZendeskTagValue(value);
    }).filter(Boolean);
    if (parts.length) result[key] = parts.join(', ');
  };

  (ticket.custom_fields || []).forEach(cf => {
    const id = String(cf.id || '');
    const meta = ticketFieldMap[id];
    if (!meta || !meta.name) return;
    for (const [target, keywords] of Object.entries(normalizedMap)) {
      if (keywords.some(k => meta.name.includes(k))) {
        setValue(target, cf.value, meta.options);
      }
    }
  });

  const tags = (ticket.tags || []).map(t => String(t || '').trim()).filter(Boolean);
  const pickTag = (prefixes) => {
    const tag = tags.find(t => prefixes.some(p => t.toLowerCase().startsWith(p)));
    return tag ? prettifyZendeskTagValue(tag) : null;
  };

  if (!result.categoria) result.categoria = pickTag(['categoria_', 'category_']);
  if (!result.documento) result.documento = pickTag(['documento_', 'document_', 'doc_']);
  if (!result.produto) result.produto = pickTag(['produto_', 'product_']);

  return {
    categoria: result.categoria || '-',
    documento: result.documento || '-',
    produto: result.produto || '-',
  };
}

function parseSpreadsheetDate(raw) {
  const value = String(raw || '').trim();
  if (!value) return null;

  const br = /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;
  const mBr = br.exec(value);
  if (mBr) {
    const day = Number(mBr[1]);
    const month = Number(mBr[2]);
    const year = Number(mBr[3]);
    const hour = Number(mBr[4] || 0);
    const minute = Number(mBr[5] || 0);
    const second = Number(mBr[6] || 0);
    const dt = new Date(year, month - 1, day, hour, minute, second);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function parsePeerReviewAnswerScore(raw) {
  const value = String(raw || '').trim();
  if (!value) return null;

  const normalized = normalizeText(value);
  const numMatch = normalized.match(/-?\d+(?:[.,]\d+)?/);
  if (numMatch) {
    const n = Number(numMatch[0].replace(',', '.'));
    if (Number.isFinite(n)) return n;
  }

  const map = [
    ['discordo totalmente', 1],
    ['discordo parcialmente', 2],
    ['nem concordo nem discordo', 3],
    ['neutro', 3],
    ['concordo parcialmente', 4],
    ['concordo totalmente', 5],
    ['muito ruim', 1],
    ['ruim', 2],
    ['regular', 3],
    ['bom', 4],
    ['otimo', 5],
    ['excelente', 5],
  ];
  for (const [label, score] of map) {
    if (normalized.includes(label)) return score;
  }

  return null;
}

function classifyPeerReviewType(raw) {
  const normalized = normalizeText(raw);
  if (!normalized) return null;
  if (normalized.includes('gestor')) return 'gestor';
  if (normalized.includes('par')) return 'par';
  return null;
}

function resolvePeerReviewColumns(headerRow) {
  const headers = (headerRow || []).map(h => String(h || '').trim());
  const normalizedHeaders = headers.map(normalizeText);

  const findIndexByTokens = (tokenGroups) => normalizedHeaders.findIndex(h =>
    tokenGroups.some(tokens => tokens.every(t => h.includes(normalizeText(t))))
  );

  const timestampIdx = findIndexByTokens([
    ['carimbo', 'data', 'hora'],
  ]);
  const emailIdx = findIndexByTokens([
    ['e-mail', 'colaborador', 'avaliado'],
    ['email', 'colaborador', 'avaliado'],
  ]);
  const reviewTypeIdx = findIndexByTokens([
    ['avaliacao', 'sera', 'como'],
    ['sua', 'avaliacao', 'como'],
  ]);

  const scoreMatchers = [
    ['essencial', 'resultados', 'bom funcionamento do time'],
    ['competencias necessarias', 'cargo'],
    ['facil trabalhar'],
    ['feliz na tecnospeed'],
    ['molho especial', 'proatividade', 'colaboracao', 'conhecimento', 'confianca'],
    ['experiencia do cliente', 'empatia', 'cordialidade'],
    ['comprometido', 'pontualidade', 'organizacao', 'prazos'],
    ['feliz na equipe'],
  ];
  const scoreIndexes = scoreMatchers
    .map(tokens => findIndexByTokens([tokens]))
    .filter(idx => idx >= 0);

  if (timestampIdx < 0 || emailIdx < 0 || reviewTypeIdx < 0 || !scoreIndexes.length) {
    const missing = [];
    if (timestampIdx < 0) missing.push('Carimbo de data/hora');
    if (emailIdx < 0) missing.push('E-mail do colaborador que sera avaliado');
    if (reviewTypeIdx < 0) missing.push('Sua avaliacao sera como');
    if (!scoreIndexes.length) missing.push('colunas de nota da avaliacao');
    const err = new Error(`Colunas obrigatorias nao encontradas na planilha: ${missing.join(', ')}`);
    err.code = 'GOOGLE_SHEETS_COLUMNS_MISSING';
    throw err;
  }

  return { timestampIdx, emailIdx, reviewTypeIdx, scoreIndexes };
}

function loadGooglePeerReviewCredentials() {
  const inline = String(process.env.GOOGLE_PEER_REVIEW_CREDENTIALS_JSON || '').trim();
  let payload = null;
  if (inline) {
    payload = JSON.parse(inline);
  } else {
    const filePath = GOOGLE_PEER_REVIEW_CREDENTIALS_FILE;
    if (!filePath || !fs.existsSync(filePath)) {
      const err = new Error('Credenciais da planilha Google nao configuradas');
      err.code = 'GOOGLE_SHEETS_NOT_CONFIGURED';
      throw err;
    }
    payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }

  if (!payload?.client_email || !payload?.private_key) {
    const err = new Error('Credenciais da planilha Google invalidas');
    err.code = 'GOOGLE_SHEETS_NOT_CONFIGURED';
    throw err;
  }

  payload.private_key = String(payload.private_key).replace(/\\n/g, '\n');
  return payload;
}

async function fetchPeerReviewsRowsFromGoogle() {
  if (!GOOGLE_PEER_REVIEW_SPREADSHEET_ID) {
    const err = new Error('Planilha de avaliacoes de pares nao configurada');
    err.code = 'GOOGLE_SHEETS_NOT_CONFIGURED';
    throw err;
  }

  const credentials = loadGooglePeerReviewCredentials();
  try {
    const auth = new google.auth.JWT({
      email: credentials.client_email,
      key: credentials.private_key,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    const spreadsheetInfo = await sheets.spreadsheets.get({
      spreadsheetId: GOOGLE_PEER_REVIEW_SPREADSHEET_ID,
      fields: 'sheets.properties',
    });
    const allSheets = spreadsheetInfo.data.sheets || [];
    const targetSheet = allSheets.find(s => String(s?.properties?.sheetId) === String(GOOGLE_PEER_REVIEW_SHEET_GID))
      || allSheets[0];
    if (!targetSheet?.properties?.title) {
      throw new Error('Nao foi possivel identificar a aba da planilha de avaliacoes de pares');
    }

    const title = targetSheet.properties.title.replace(/'/g, "''");
    const range = `'${title}'!${GOOGLE_PEER_REVIEW_RANGE}`;
    const valuesResp = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_PEER_REVIEW_SPREADSHEET_ID,
      range,
    });

    return valuesResp.data.values || [];
  } catch (e) {
    const rawMsg = String(e?.message || e || '');
    if (/permission|forbidden|insufficient|not have permission/i.test(rawMsg)) {
      const err = new Error(`Sem permissao de leitura na planilha. Compartilhe a planilha com ${credentials.client_email}`);
      err.code = 'GOOGLE_SHEETS_PERMISSION';
      throw err;
    }
    throw e;
  }
}

function buildPeerReviewsRanking(consultants, period, sheetRows) {
  const safeConsultants = Array.isArray(consultants) ? consultants : [];
  const rows = Array.isArray(sheetRows) ? sheetRows : [];
  const scoreScaleCandidates = [5, 10, 100];

  const acc = {};
  const consultantByEmail = {};
  const consultantByLocalPart = {};
  safeConsultants.forEach(c => {
    const cid = String(c.id);
    acc[cid] = {
      par_points: 0,
      par_items: 0,
      par_count: 0,
      gestor_points: 0,
      gestor_items: 0,
      gestor_count: 0,
    };
    const em = normalizeEmail(c.email);
    if (em) consultantByEmail[em] = cid;
    const login = normalizeText(c.login);
    if (login) consultantByLocalPart[login] = cid;
    if (em.includes('@')) consultantByLocalPart[em.split('@')[0]] = cid;
  });

  if (!rows.length) {
    return {
      average: null,
      total: 0,
      ranking: safeConsultants.map(c => ({
        consultant_id: c.id,
        consultant_name: c.name,
        score: null,
        total: 0,
        par_score: null,
        par_total: 0,
        gestor_score: null,
        gestor_total: 0,
      })).sort(sortRankingRows),
    };
  }

  const [headerRow, ...dataRows] = rows;
  const columns = resolvePeerReviewColumns(headerRow);
  let observedMax = 0;

  for (const row of dataRows) {
    const createdAt = parseSpreadsheetDate(row[columns.timestampIdx]);
    if (!createdAt || monthRefFromDate(createdAt) !== period.month) continue;

    const email = normalizeEmail(row[columns.emailIdx]);
    const localPart = email.includes('@') ? email.split('@')[0] : email;
    const consultantId = consultantByEmail[email] || consultantByLocalPart[localPart];
    if (!consultantId || !acc[consultantId]) continue;

    const reviewType = classifyPeerReviewType(row[columns.reviewTypeIdx]);
    if (!reviewType) continue;

    let points = 0;
    let items = 0;
    for (const idx of columns.scoreIndexes) {
      const n = parsePeerReviewAnswerScore(row[idx]);
      if (n === null || n === undefined || Number.isNaN(Number(n))) continue;
      const score = Number(n);
      points += score;
      items += 1;
      if (score > observedMax) observedMax = score;
    }
    if (!items) continue;

    if (reviewType === 'par') {
      acc[consultantId].par_points += points;
      acc[consultantId].par_items += items;
      acc[consultantId].par_count += 1;
    } else if (reviewType === 'gestor') {
      acc[consultantId].gestor_points += points;
      acc[consultantId].gestor_items += items;
      acc[consultantId].gestor_count += 1;
    }
  }

  const scaleMax = scoreScaleCandidates.find(v => observedMax <= v) || Math.max(5, observedMax || 5);
  const ranking = safeConsultants.map(c => {
    const a = acc[String(c.id)] || {};
    const parItems = Number(a.par_items || 0);
    const gestorItems = Number(a.gestor_items || 0);
    const parScore = parItems > 0 ? round2((Number(a.par_points || 0) / (parItems * scaleMax)) * 100) : null;
    const gestorScore = gestorItems > 0 ? round2((Number(a.gestor_points || 0) / (gestorItems * scaleMax)) * 100) : null;
    const allItems = parItems + gestorItems;
    const allPoints = Number(a.par_points || 0) + Number(a.gestor_points || 0);
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: allItems > 0 ? round2((allPoints / (allItems * scaleMax)) * 100) : null,
      total: Number(a.par_count || 0) + Number(a.gestor_count || 0),
      par_score: parScore,
      par_total: Number(a.par_count || 0),
      gestor_score: gestorScore,
      gestor_total: Number(a.gestor_count || 0),
    };
  }).sort(sortRankingRows);

  const totals = ranking.reduce((s, r) => {
    s.total += Number(r.total || 0);
    if (r.score !== null && r.total > 0) s.sum += Number(r.score) * Number(r.total || 0);
    return s;
  }, { sum: 0, total: 0 });

  return {
    average: totals.total > 0 ? round2(totals.sum / totals.total) : null,
    total: totals.total,
    ranking,
  };
}

function buildEmptyConsultantsCards() {
  return {
    csat: { average: null, total: 0, good: 0, bad: 0, ranking: [], source: 'no_data', error: null },
    atendimento: { average: null, total: 0, ranking: [] },
    negativados: { average: null, total: 0, ranking: [] },
    pdca: { average: null, total: 0, ranking: [] },
    total_atendimentos: { average: null, total: 0, ranking: [], source: 'no_data', error: null },
    avaliacoes_pares: { average: null, total: 0, ranking: [], source: 'no_data', error: null },
    indice_tecnico: { average: null, total: 0, ranking: [], source: 'calculated', error: null },
  };
}

function sortRankingRows(a, b) {
  if (a.score === null && b.score === null) return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
  if (a.score === null) return 1;
  if (b.score === null) return -1;
  if (b.score !== a.score) return b.score - a.score;
  if ((b.total || 0) !== (a.total || 0)) return (b.total || 0) - (a.total || 0);
  return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
}

async function fetchZendeskCsatByPeriod(cfg, startUtc, endUtc) {
  const startTime = Math.floor(startUtc.getTime() / 1000);
  let nextPath = `/api/v2/satisfaction_ratings.json?start_time=${startTime}`;
  const all = [];
  while (nextPath) {
    const pageData = await zendeskRequest(nextPath, cfg, 'CSAT_RATINGS_PERIOD');
    all.push(...(pageData.satisfaction_ratings || []));
    if (!pageData.next_page) break;
    try {
      const u = new URL(pageData.next_page);
      nextPath = u.pathname + u.search;
    } catch (_) {
      break;
    }
  }
  return all.filter(r => {
    const d = new Date(r.created_at);
    return !Number.isNaN(d.getTime()) && d >= startUtc && d < endUtc;
  });
}

async function fetchZendeskCsatByPeriodPerAssignee(cfg, startUtc, endUtc, assigneeIds) {
  const ids = [...new Set((assigneeIds || []).map(v => String(v).trim()).filter(Boolean))];
  if (!ids.length) return [];

  const startTime = Math.floor(startUtc.getTime() / 1000);
  const all = [];
  const seen = new Set();
  const failures = [];

  for (const aid of ids) {
    let nextPath = `/api/v2/satisfaction_ratings.json?assignee_id=${encodeURIComponent(aid)}&start_time=${startTime}`;
    let guard = 0;
    try {
      while (nextPath && guard < 60) {
        guard += 1;
        const pageData = await zendeskRequest(nextPath, cfg, `CSAT_RATINGS_ASSIGNEE_${aid}`);
        (pageData.satisfaction_ratings || []).forEach(r => {
          const rid = String(r.id || `${r.ticket_id || ''}-${r.assignee_id || ''}-${r.created_at || ''}`);
          if (seen.has(rid)) return;
          seen.add(rid);
          all.push(r);
        });
        if (!pageData.next_page) break;
        try {
          const u = new URL(pageData.next_page);
          nextPath = u.pathname + u.search;
        } catch (_) {
          break;
        }
      }
    } catch (e) {
      failures.push({ aid, error: String(e?.message || 'erro desconhecido') });
    }
  }

  if (failures.length) {
    const err = new Error(`ASSIGNEE_QUERY_FAILED: ${failures.length}/${ids.length} falharam (${failures.slice(0, 3).map(f => `${f.aid}: ${f.error}`).join(' | ')})`);
    err.code = 'ASSIGNEE_QUERY_FAILED';
    throw err;
  }

  const allowed = new Set(ids.map(String));
  return all.filter(r => {
    const d = new Date(r.created_at);
    const aid = String(r.assignee_id || '').trim();
    return !Number.isNaN(d.getTime()) && d >= startUtc && d < endUtc && (!allowed.size || allowed.has(aid));
  });
}

async function fetchZendeskCsatCountsBySearch(cfg, startUtc, endUtc, allowedAssigneeSet) {
  const start = new Date(startUtc).toISOString().slice(0, 10);
  const end = new Date(endUtc).toISOString().slice(0, 10);
  const acc = {};

  for (const score of ['good', 'bad']) {
    let nextPath = `/api/v2/search.json?query=${encodeURIComponent(`type:ticket satisfaction:${score} created>=${start} created<=${end}`)}&per_page=100`;
    let guard = 0;
    while (nextPath && guard < 60) {
      guard += 1;
      const pageData = await zendeskRequest(nextPath, cfg, `CSAT_COUNTS_SEARCH_${score.toUpperCase()}`);
      const results = Array.isArray(pageData?.results) ? pageData.results : [];
      results.forEach(t => {
        const aid = String(t?.assignee_id || '').trim();
        if (!aid) return;
        if (allowedAssigneeSet && allowedAssigneeSet.size && !allowedAssigneeSet.has(aid)) return;
        if (!acc[aid]) acc[aid] = { good: 0, bad: 0 };
        if (score === 'good') acc[aid].good += 1;
        if (score === 'bad') acc[aid].bad += 1;
      });

      if (!pageData?.next_page) break;
      try {
        const u = new URL(pageData.next_page);
        nextPath = u.pathname + u.search;
      } catch (_) {
        break;
      }
    }
  }

  return Object.entries(acc).map(([id_assignee_zendesk, v]) => ({
    id_assignee_zendesk: String(id_assignee_zendesk),
    good: Number(v.good || 0),
    bad: Number(v.bad || 0),
  }));
}

function buildScoreRanking(consultants, assigneeToConsultant, rows) {
  const acc = {};
  consultants.forEach(c => {
    acc[c.id] = { sum: 0, total: 0 };
  });

  rows.forEach(r => {
    const cid = assigneeToConsultant[String(r.id_assignee_zendesk)];
    if (!cid || !acc[cid]) return;
    acc[cid].sum += Number(r.sum_score || 0);
    acc[cid].total += Number(r.total || 0);
  });

  const ranking = consultants.map(c => {
    const a = acc[c.id] || { sum: 0, total: 0 };
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: a.total > 0 ? round2(a.sum / a.total) : null,
      total: a.total,
    };
  }).sort(sortRankingRows);

  const totals = Object.values(acc).reduce((s, a) => {
    s.sum += a.sum;
    s.total += a.total;
    return s;
  }, { sum: 0, total: 0 });

  return {
    average: totals.total > 0 ? round2(totals.sum / totals.total) : null,
    total: totals.total,
    ranking,
  };
}

function calcNegativadosScore(totalNegativos) {
  const total = Math.max(0, Number(totalNegativos || 0));
  return round2(Math.max(0, 100 - (total * 10)));
}

function buildNegativadosRanking(consultants, assigneeToConsultant, rows) {
  const acc = {};
  consultants.forEach(c => {
    acc[c.id] = { total: 0 };
  });

  (rows || []).forEach(r => {
    const cid = assigneeToConsultant[String(r.id_assignee_zendesk)];
    if (!cid || !acc[cid]) return;
    acc[cid].total += Number(r.total || 0);
  });

  const ranking = consultants.map(c => {
    const total = Number(acc[c.id]?.total || 0);
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: calcNegativadosScore(total),
      total,
    };
  }).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (a.total !== b.total) return a.total - b.total;
    return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
  });

  const totalRegistros = ranking.reduce((s, r) => s + Number(r.total || 0), 0);
  const average = ranking.length
    ? round2(ranking.reduce((s, r) => s + Number(r.score || 0), 0) / ranking.length)
    : null;

  return {
    average,
    total: totalRegistros,
    ranking,
  };
}

function buildCsatRanking(consultants, assigneeToConsultant, ratings) {
  const acc = {};
  consultants.forEach(c => {
    acc[c.id] = { good: 0, bad: 0 };
  });

  ratings.forEach(r => {
    const cid = assigneeToConsultant[String(r.assignee_id || '')];
    if (!cid || !acc[cid]) return;
    const score = String(r.score || '').toLowerCase();
    if (score === 'good') acc[cid].good += 1;
    if (score === 'bad') acc[cid].bad += 1;
  });

  const ranking = consultants.map(c => {
    const a = acc[c.id] || { good: 0, bad: 0 };
    const total = a.good + a.bad;
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: total > 0 ? round2((a.good / total) * 100) : null,
      total,
      good: a.good,
      bad: a.bad,
    };
  }).sort(sortRankingRows);

  const totals = Object.values(acc).reduce((s, a) => {
    s.good += a.good;
    s.bad += a.bad;
    return s;
  }, { good: 0, bad: 0 });
  const total = totals.good + totals.bad;

  return {
    average: total > 0 ? round2((totals.good / total) * 100) : null,
    total,
    good: totals.good,
    bad: totals.bad,
    ranking,
  };
}

function buildCsatRankingFromCountRows(consultants, assigneeToConsultant, rows) {
  const acc = {};
  consultants.forEach(c => {
    acc[c.id] = { good: 0, bad: 0 };
  });

  (rows || []).forEach(r => {
    const cid = assigneeToConsultant[String(r.id_assignee_zendesk || '')];
    if (!cid || !acc[cid]) return;
    acc[cid].good += Number(r.good || 0);
    acc[cid].bad += Number(r.bad || 0);
  });

  const ranking = consultants.map(c => {
    const a = acc[c.id] || { good: 0, bad: 0 };
    const total = Number(a.good || 0) + Number(a.bad || 0);
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: total > 0 ? round2((Number(a.good || 0) / total) * 100) : null,
      total,
      good: Number(a.good || 0),
      bad: Number(a.bad || 0),
    };
  }).sort(sortRankingRows);

  const totals = Object.values(acc).reduce((s, a) => {
    s.good += Number(a.good || 0);
    s.bad += Number(a.bad || 0);
    return s;
  }, { good: 0, bad: 0 });
  const total = totals.good + totals.bad;

  return {
    average: total > 0 ? round2((totals.good / total) * 100) : null,
    total,
    good: totals.good,
    bad: totals.bad,
    ranking,
  };
}

function buildCountRanking(consultants, consultantTotals, referenceConsultants = null) {
  const rankedConsultants = Array.isArray(consultants) ? consultants : [];
  const baseConsultants = Array.isArray(referenceConsultants) && referenceConsultants.length
    ? referenceConsultants
    : rankedConsultants;
  const n1BaseConsultants = baseConsultants.filter(c => String(c?.user_type || '').trim() === 'n1');
  const consultantsForAverage = n1BaseConsultants;
  const teamTotal = consultantsForAverage.reduce((sum, c) => sum + Number((consultantTotals && consultantTotals[c.id]) || 0), 0);
  const teamSize = consultantsForAverage.length;
  const teamAverage = teamSize ? round2(teamTotal / teamSize) : null;

  const ranking = rankedConsultants.map(c => {
    const total = Number((consultantTotals && consultantTotals[c.id]) || 0);
    const note = calcTotalAtendimentosNote(teamTotal, teamSize, total);
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: round2(note * 10),
      total,
      score_note: note,
      team_average: teamAverage,
      index_ratio: teamAverage && teamAverage > 0 ? round2(total / teamAverage) : null,
    };
  }).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.total !== a.total) return b.total - a.total;
    return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
  });

  const total = teamTotal;
  const average = teamAverage;

  return { average, total, ranking };
}

function buildTechnicalIndexCard(cards, consultants) {
  const safeConsultants = Array.isArray(consultants) ? consultants : [];
  const negativadosMap = {};
  const atendimentoMap = {};
  const totalAtMap = {};
  const paresMap = {};

  (cards?.negativados?.ranking || []).forEach(r => { negativadosMap[String(r.consultant_id)] = r; });
  (cards?.atendimento?.ranking || []).forEach(r => { atendimentoMap[String(r.consultant_id)] = r; });
  (cards?.total_atendimentos?.ranking || []).forEach(r => { totalAtMap[String(r.consultant_id)] = r; });
  (cards?.avaliacoes_pares?.ranking || []).forEach(r => { paresMap[String(r.consultant_id)] = r; });

  const useNegativados = true;
  const useAtendimento = true;
  const useTotalAtendimentos = cards?.total_atendimentos?.source !== 'not_configured' && cards?.total_atendimentos?.source !== 'error';
  const usePares = cards?.avaliacoes_pares?.source !== 'not_configured' && cards?.avaliacoes_pares?.source !== 'error';

  const toPct = (v) => {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return 0;
    const n = Number(v);
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(100, n));
  };

  const WEIGHTS = {
    par: 0.10,
    gestor: 0.20,
    atendimento: 0.40,
    negativados: 0.20,
    totalAtendimentos: 0.10,
  };

  const ranking = safeConsultants.map(c => {
    const cid = String(c.id);
    const negativadosScore = useNegativados ? toPct(negativadosMap[cid]?.score) : 0;
    const atendimentoScore = useAtendimento ? toPct(atendimentoMap[cid]?.score) : 0;
    const parScore = usePares
      ? toPct(paresMap[cid]?.par_score ?? paresMap[cid]?.score)
      : 0;
    const gestorScore = usePares
      ? toPct(paresMap[cid]?.gestor_score ?? paresMap[cid]?.score)
      : 0;
    const totalAtendimentosRaw = Number(totalAtMap[cid]?.total || totalAtMap[cid]?.score || 0);
    const totalAtendimentosScore = useTotalAtendimentos
      ? toPct(totalAtMap[cid]?.score)
      : 0;

    // Índice técnico = (Par*0,10) + (Gestor*0,20) + (Atendimento*0,40) + (CSAT*0,20) + (TotalAtendimentos*0,10)
    const score = round2(
      (parScore * WEIGHTS.par)
      + (gestorScore * WEIGHTS.gestor)
      + (atendimentoScore * WEIGHTS.atendimento)
      + (negativadosScore * WEIGHTS.negativados)
      + (totalAtendimentosScore * WEIGHTS.totalAtendimentos)
    );

    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score,
      total: 5,
      negativados_score: negativadosScore,
      total_atendimentos_score: totalAtendimentosScore,
      total_atendimentos_raw: totalAtendimentosRaw,
      atendimento_score: atendimentoScore,
      par_score: parScore,
      gestor_score: gestorScore,
      pares_score: round2((parScore * WEIGHTS.par) + (gestorScore * WEIGHTS.gestor)),
    };
  }).sort(sortRankingRows);

  const average = ranking.length
    ? round2(ranking.reduce((s, r) => s + Number(r.score || 0), 0) / ranking.length)
    : null;

  const source = 'calculated';
  const errors = [
    cards?.total_atendimentos?.source === 'error' ? cards?.total_atendimentos?.error : null,
    cards?.avaliacoes_pares?.source === 'error' ? cards?.avaliacoes_pares?.error : null,
  ].filter(Boolean);

  return {
    average,
    total: ranking.length,
    ranking,
    source,
    error: errors.length ? errors.join(' | ') : null,
  };
}

function summarizeHistorySource(rows) {
  const sources = new Set((rows || []).map(r => String(r.source || '').trim()).filter(Boolean));
  const errors = [...new Set((rows || []).map(r => String(r.error || '').trim()).filter(Boolean))];
  let source = 'history';
  if (sources.has('error')) source = 'error';
  else if (sources.has('not_configured')) source = 'not_configured';
  else if (sources.size === 1) source = [...sources][0];
  return {
    source,
    error: source === 'error' ? (errors.join(' | ') || null) : null,
  };
}

function buildConsultantsCardsFromHistory(consultants, historyRows, referenceConsultants = null) {
  const cards = buildEmptyConsultantsCards();
  const safeConsultants = Array.isArray(consultants) ? consultants : [];
  const baseConsultants = Array.isArray(referenceConsultants) && referenceConsultants.length
    ? referenceConsultants
    : safeConsultants;
  const consultantById = {};
  [...safeConsultants, ...baseConsultants].forEach(c => { consultantById[String(c.id)] = c; });

  const metricRows = {
    csat: [],
    atendimento: [],
    negativados: [],
    pdca: [],
    total_atendimentos: [],
    avaliacoes_pares: [],
  };

  (historyRows || []).forEach(row => {
    const metric = String(row.metrica || '').trim();
    if (!metricRows[metric]) return;
    const cid = String(row.consultant_id || '');
    const fallbackName = consultantById[cid]?.name || `ID ${cid}`;
      metricRows[metric].push({
        consultant_id: cid,
        consultant_name: row.consultant_name || fallbackName,
        score: metric === 'negativados'
          ? calcNegativadosScore(Number(row.total || 0))
          : (row.score === null || row.score === undefined ? null : round2(Number(row.score))),
        total: Number(row.total || 0),
        good: Number(row.positivos || 0),
        bad: Number(row.negativos || 0),
      source: row.fonte ? String(row.fonte) : null,
      error: row.erro ? String(row.erro) : null,
    });
  });

  CONSULTANT_RANK_METRICS.forEach(metric => {
    const seen = new Set(metricRows[metric].map(r => String(r.consultant_id)));
    safeConsultants.forEach(c => {
      const cid = String(c.id);
      if (seen.has(cid)) return;
      metricRows[metric].push({
        consultant_id: cid,
        consultant_name: c.name,
        score: metric === 'total_atendimentos'
          ? 0
          : (metric === 'negativados' ? 100 : null),
        total: 0,
        good: 0,
        bad: 0,
        source: null,
        error: null,
      });
    });
  });

  const csatRanking = metricRows.csat.map(r => ({
    consultant_id: r.consultant_id,
    consultant_name: r.consultant_name,
    score: r.score,
    total: Number(r.total || 0),
    good: Number(r.good || 0),
    bad: Number(r.bad || 0),
  })).sort(sortRankingRows);
  const csatGood = csatRanking.reduce((s, r) => s + Number(r.good || 0), 0);
  const csatBad = csatRanking.reduce((s, r) => s + Number(r.bad || 0), 0);
  const csatScored = csatRanking.filter(r => r.score !== null && r.score !== undefined);
  const csatAverage = csatScored.length
    ? round2(csatScored.reduce((s, r) => s + Number(r.score || 0), 0) / csatScored.length)
    : null;
  const csatSource = summarizeHistorySource(metricRows.csat);
  cards.csat = {
    average: csatAverage,
    total: csatRanking.reduce((s, r) => s + Number(r.total || 0), 0),
    good: csatGood,
    bad: csatBad,
    ranking: csatRanking,
    source: csatSource.source,
    error: csatSource.error,
  };

  ['atendimento', 'negativados', 'pdca', 'avaliacoes_pares'].forEach(metric => {
    const ranking = metricRows[metric].map(r => ({
      consultant_id: r.consultant_id,
      consultant_name: r.consultant_name,
      score: r.score,
      total: Number(r.total || 0),
    })).sort(metric === 'negativados'
      ? ((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          if (a.total !== b.total) return a.total - b.total;
          return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
        })
      : sortRankingRows);
    const totals = ranking.reduce((s, r) => {
      s.total += Number(r.total || 0);
      if (metric === 'negativados') {
        if (r.score !== null) s.sum += Number(r.score);
      } else if (r.score !== null && r.total > 0) {
        s.sum += Number(r.score) * Number(r.total || 0);
      }
      return s;
    }, { sum: 0, total: 0 });
    cards[metric] = {
      average: metric === 'negativados'
        ? (ranking.length ? round2(totals.sum / ranking.length) : null)
        : (totals.total > 0 ? round2(totals.sum / totals.total) : null),
      total: totals.total,
      ranking,
    };
  });

  const totalConsultantTotals = {};
  metricRows.total_atendimentos.forEach(r => {
    totalConsultantTotals[String(r.consultant_id)] = Number(r.total || 0);
  });
  const totalRankingCard = buildCountRanking(safeConsultants, totalConsultantTotals, baseConsultants);
  const totalAtendimentosSource = summarizeHistorySource(metricRows.total_atendimentos);
  cards.total_atendimentos = {
    average: totalRankingCard.average,
    total: totalRankingCard.total,
    ranking: totalRankingCard.ranking,
    source: totalAtendimentosSource.source,
    error: totalAtendimentosSource.error,
  };

  cards.indice_tecnico = buildTechnicalIndexCard(cards, safeConsultants);

  return cards;
}

function buildConsultantsCardsFromCsatHistoryRows(consultants, historyRows, referenceConsultants = null) {
  const cards = buildEmptyConsultantsCards();
  const safeConsultants = Array.isArray(consultants) ? consultants : [];
  const baseConsultants = Array.isArray(referenceConsultants) && referenceConsultants.length
    ? referenceConsultants
    : safeConsultants;
  const rowByConsultant = {};

  (historyRows || []).forEach(r => {
    rowByConsultant[String(r.consultant_id)] = r;
  });

  const csatRows = safeConsultants.map(c => {
    const row = rowByConsultant[String(c.id)] || {};
    const good = Number(row.positivos || 0);
    const bad = Number(row.negativos || 0);
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: row.csat_percent === null || row.csat_percent === undefined ? null : round2(Number(row.csat_percent)),
      total: Number(row.total_avaliacoes || 0),
      good,
      bad,
      source: row.fonte ? String(row.fonte) : null,
      error: row.erro ? String(row.erro) : null,
    };
  }).sort(sortRankingRows);

  const csatGood = csatRows.reduce((s, r) => s + Number(r.good || 0), 0);
  const csatBad = csatRows.reduce((s, r) => s + Number(r.bad || 0), 0);
  const csatScored = csatRows.filter(r => r.score !== null && r.score !== undefined);
  const csatAverage = csatScored.length
    ? round2(csatScored.reduce((s, r) => s + Number(r.score || 0), 0) / csatScored.length)
    : null;
  const csatSource = summarizeHistorySource(csatRows);
  cards.csat = {
    average: csatAverage,
    total: csatRows.reduce((s, r) => s + Number(r.total || 0), 0),
    good: csatGood,
    bad: csatBad,
    ranking: csatRows.map(r => ({
      consultant_id: r.consultant_id,
      consultant_name: r.consultant_name,
      score: r.score,
      total: r.total,
      good: r.good,
      bad: r.bad,
    })),
    source: csatSource.source,
    error: csatSource.error,
  };

  const buildScoreCard = (scoreField, totalField, opts = {}) => {
    const isNegativados = !!opts.negativados;
    const ranking = safeConsultants.map(c => {
      const row = rowByConsultant[String(c.id)] || {};
      const total = Number(row[totalField] || 0);
      const score = isNegativados
        ? calcNegativadosScore(total)
        : (row[scoreField] === null || row[scoreField] === undefined ? null : round2(Number(row[scoreField])));
      return {
        consultant_id: c.id,
        consultant_name: c.name,
        score,
        total,
      };
    }).sort(isNegativados
      ? ((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          if (a.total !== b.total) return a.total - b.total;
          return a.consultant_name.localeCompare(b.consultant_name, 'pt-BR');
        })
      : sortRankingRows);

    const totals = ranking.reduce((s, r) => {
      s.total += Number(r.total || 0);
      if (isNegativados) {
        if (r.score !== null) s.sum += Number(r.score);
      } else if (r.score !== null && r.total > 0) {
        s.sum += Number(r.score) * Number(r.total || 0);
      }
      return s;
    }, { sum: 0, total: 0 });

    return {
      average: isNegativados
        ? (ranking.length ? round2(totals.sum / ranking.length) : null)
        : (totals.total > 0 ? round2(totals.sum / totals.total) : null),
      total: totals.total,
      ranking,
    };
  };

  cards.atendimento = buildScoreCard('atendimento_percent', 'atendimento_total');
  cards.negativados = buildScoreCard('negativados_percent', 'negativados_total', { negativados: true });
  cards.pdca = buildScoreCard('pdca_percent', 'pdca_total');
  const peerRanking = safeConsultants.map(c => {
    const row = rowByConsultant[String(c.id)] || {};
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      score: row.avaliacoes_pares_percent === null || row.avaliacoes_pares_percent === undefined
        ? null
        : round2(Number(row.avaliacoes_pares_percent)),
      total: Number(row.avaliacoes_pares_total || 0),
      par_score: row.avaliacoes_pares_par_percent === null || row.avaliacoes_pares_par_percent === undefined
        ? null
        : round2(Number(row.avaliacoes_pares_par_percent)),
      par_total: Number(row.avaliacoes_pares_par_total || 0),
      gestor_score: row.avaliacoes_pares_gestor_percent === null || row.avaliacoes_pares_gestor_percent === undefined
        ? null
        : round2(Number(row.avaliacoes_pares_gestor_percent)),
      gestor_total: Number(row.avaliacoes_pares_gestor_total || 0),
      source: row.fonte_avaliacoes_pares ? String(row.fonte_avaliacoes_pares) : null,
      error: row.erro_avaliacoes_pares ? String(row.erro_avaliacoes_pares) : null,
    };
  }).sort(sortRankingRows);
  const peerTotals = peerRanking.reduce((s, r) => {
    s.total += Number(r.total || 0);
    if (r.score !== null && r.total > 0) s.sum += Number(r.score) * Number(r.total || 0);
    return s;
  }, { sum: 0, total: 0 });
  const peerSource = summarizeHistorySource(peerRanking);
  cards.avaliacoes_pares = {
    average: peerTotals.total > 0 ? round2(peerTotals.sum / peerTotals.total) : null,
    total: peerTotals.total,
    ranking: peerRanking.map(r => ({
      consultant_id: r.consultant_id,
      consultant_name: r.consultant_name,
      score: r.score,
      total: r.total,
      par_score: r.par_score,
      par_total: r.par_total,
      gestor_score: r.gestor_score,
      gestor_total: r.gestor_total,
    })),
    source: peerSource.source,
    error: peerSource.error,
  };

  const totalConsultantTotals = {};
  baseConsultants.forEach(c => {
    const row = rowByConsultant[String(c.id)] || {};
    totalConsultantTotals[String(c.id)] = Number(row.total_atendimentos || 0);
  });
  const totalRankingCard = buildCountRanking(safeConsultants, totalConsultantTotals, baseConsultants);
  const totalHistorySourceRows = safeConsultants.map(c => {
    const row = rowByConsultant[String(c.id)] || {};
    return {
      consultant_id: c.id,
      consultant_name: c.name,
      source: row.fonte_total_atendimentos ? String(row.fonte_total_atendimentos) : null,
      error: row.erro_total_atendimentos ? String(row.erro_total_atendimentos) : null,
    };
  });
  const totalAtendimentosSource = summarizeHistorySource(totalHistorySourceRows);
  cards.total_atendimentos = {
    average: totalRankingCard.average,
    total: totalRankingCard.total,
    ranking: totalRankingCard.ranking.map(r => ({
      consultant_id: r.consultant_id,
      consultant_name: r.consultant_name,
      score: r.score,
      total: r.total,
      score_note: r.score_note,
      team_average: r.team_average,
      index_ratio: r.index_ratio,
    })),
    source: totalAtendimentosSource.source,
    error: totalAtendimentosSource.error,
  };

  cards.indice_tecnico = buildTechnicalIndexCard(cards, safeConsultants);

  return cards;
}

function buildMyMetricView(card, consultantId) {
  const ranking = Array.isArray(card?.ranking) ? card.ranking : [];
  const idx = ranking.findIndex(r => String(r.consultant_id) === String(consultantId));
  const row = idx >= 0 ? ranking[idx] : null;
  const maskedRanking = ranking.map((item, itemIdx) => {
    const isMe = String(item.consultant_id) === String(consultantId);
    const rankPosition = Number(item.rank_position || (itemIdx + 1));
    return {
      ...item,
      consultant_id: isMe ? item.consultant_id : null,
      consultant_name: isMe
        ? (item.consultant_name || 'Voce')
        : 'Consultor',
      photo_url: isMe ? (item.photo_url || null) : null,
      rank_position: rankPosition,
      is_me: isMe,
    };
  });
  return {
    average: card?.average ?? null,
    total: card?.total ?? 0,
    source: card?.source || null,
    error: card?.error || null,
    position: idx >= 0 ? (idx + 1) : null,
    total_consultants: ranking.length,
    ranking: maskedRanking,
    me: row ? {
      consultant_id: row.consultant_id,
      consultant_name: row.consultant_name || null,
      score: row.score ?? null,
      total: Number(row.total || 0),
      good: Number(row.good || 0),
      bad: Number(row.bad || 0),
      par_score: row.par_score ?? null,
      par_total: Number(row.par_total || 0),
      gestor_score: row.gestor_score ?? null,
      gestor_total: Number(row.gestor_total || 0),
      negativados_score: row.negativados_score ?? null,
      total_atendimentos_score: row.total_atendimentos_score ?? null,
      total_atendimentos_raw: Number(row.total_atendimentos_raw || 0),
      atendimento_score: row.atendimento_score ?? null,
      pares_score: row.pares_score ?? null,
    } : null,
  };
}

async function fetchZendeskTicketsCountByAssigneeAndPeriod(cfg, assigneeId, startUtc, endUtc) {
  const startDate = startUtc.toISOString().slice(0, 10);
  const endDate = endUtc.toISOString().slice(0, 10);
  const query = `type:ticket assignee_id:${assigneeId} status:solved solved>=${startDate} solved<${endDate}`;

  const firstPage = await zendeskRequest(`/api/v2/search.json?query=${encodeURIComponent(query)}`, cfg, 'ATENDIMENTOS_MES_COUNT');
  if (typeof firstPage.count === 'number') return Number(firstPage.count);

  let total = Array.isArray(firstPage.results) ? firstPage.results.length : 0;
  let nextPath = null;
  if (firstPage.next_page) {
    try {
      const u = new URL(firstPage.next_page);
      nextPath = u.pathname + u.search;
    } catch (_) {
      nextPath = null;
    }
  }

  while (nextPath) {
    const page = await zendeskRequest(nextPath, cfg, 'ATENDIMENTOS_MES_COUNT');
    total += Array.isArray(page.results) ? page.results.length : 0;
    if (!page.next_page) break;
    try {
      const u = new URL(page.next_page);
      nextPath = u.pathname + u.search;
    } catch (_) {
      nextPath = null;
    }
  }

  return total;
}

async function loadConsultantsScopeForUser(user, rawGroupIds) {
  const definitions = await fetchGroupDefinitions();
  const definitionMap = new Map(definitions.map(def => [String(def.id), def]));
  const linkedActualIds = new Set(
    definitions
      .filter(def => def.kind === 'aggregator')
      .flatMap(def => def.linked_group_ids || [])
      .map(v => String(v))
  );

  let availableDefinitions = [];
  if (user.role === 'manager') {
    const managedIds = new Set(await getManagedGroupIdsByUserId(user.id));
    availableDefinitions = definitions.filter(def => managedIds.has(String(def.id)));
  } else if (user.role === 'consultant') {
    const { rows: myMembershipRows } = await db.query(
      'SELECT DISTINCT grupo_id::text AS id FROM membros_grupo WHERE usuario_id = $1::uuid',
      [user.id]
    );
    const myActualGroupIds = new Set(myMembershipRows.map(r => String(r.id)));
    availableDefinitions = definitions.filter((def) => {
      if (def.kind === 'aggregator') {
        return (def.linked_group_ids || []).some(groupId => myActualGroupIds.has(String(groupId)));
      }
      return myActualGroupIds.has(String(def.id)) && !linkedActualIds.has(String(def.id));
    });
  } else {
    availableDefinitions = definitions.filter(def => def.kind === 'aggregator' || !linkedActualIds.has(String(def.id)));
  }

  const availableGroupIds = new Set(availableDefinitions.map(def => String(def.id)));
  if (!availableDefinitions.length) {
    return {
      groups: [],
      consultants: [],
      selectedGroupIds: [],
      selectedGroupNames: [],
    };
  }

  const selectedGroupIds = parseCsvQueryIds(rawGroupIds);
  const invalidGroup = selectedGroupIds.find(id => !availableGroupIds.has(String(id)));
  if (invalidGroup) {
    const err = new Error('Grupo invalido para o usuario logado');
    err.statusCode = 403;
    throw err;
  }

  const availableActualGroupIds = dedupeStringIds(
    availableDefinitions.flatMap(def => resolveDefinitionActualGroupIds(def, definitionMap))
  );
  const membershipRows = await fetchConsultantsByActualGroupIds(availableActualGroupIds);
  const consultantsByActualGroup = {};
  membershipRows.forEach((row) => {
    const groupId = String(row.actual_group_id || '');
    if (!consultantsByActualGroup[groupId]) consultantsByActualGroup[groupId] = [];
    consultantsByActualGroup[groupId].push({
      id: row.consultant_id,
      name: row.consultant_name,
      user_type: row.user_type ? String(row.user_type) : null,
      requester_id: row.requester_id ? String(row.requester_id) : null,
      assignee_ids: dedupeStringIds(row.assignee_ids || []),
    });
  });

  const groups = availableDefinitions.map((def) => {
    const resolvedGroupIds = resolveDefinitionActualGroupIds(def, definitionMap);
    const consultantsMap = new Map();
    resolvedGroupIds.forEach((groupId) => {
      (consultantsByActualGroup[groupId] || []).forEach((consultant) => {
        consultantsMap.set(String(consultant.id), consultant);
      });
    });
    return {
      id: String(def.id),
      id_zendesk: def.id_zendesk || null,
      name: def.name,
      kind: def.kind,
      linked_group_ids: def.linked_group_ids || [],
      linked_group_names: def.linked_group_names || [],
      resolved_group_ids: resolvedGroupIds,
      consultants_count: consultantsMap.size,
      consultants: [...consultantsMap.values()],
    };
  }).sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));

  let consultants = [];
  if (selectedGroupIds.length) {
    const selectedActualGroupIds = dedupeStringIds(
      selectedGroupIds.flatMap((groupId) => {
        const def = definitionMap.get(String(groupId));
        return resolveDefinitionActualGroupIds(def, definitionMap);
      })
    );
    try {
      const { rows } = await db.query(`
        SELECT
          u.id,
          u.login,
          u.nome AS name,
          u.email,
          u.tipo_usuario AS user_type,
          u.foto_url AS photo_url,
          u.id_zendesk AS requester_id,
          COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids,
          COALESCE(array_agg(DISTINCT g.id::text), '{}') AS group_ids,
          COALESCE(array_agg(DISTINCT g.nome), '{}') AS group_names
        FROM usuarios u
        JOIN membros_grupo mg ON mg.usuario_id = u.id
        JOIN grupos g ON g.id = mg.grupo_id
        LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
        WHERE mg.grupo_id = ANY($1::uuid[])
          AND u.id_zendesk IS NOT NULL
        GROUP BY u.id, u.login, u.nome, u.email, u.tipo_usuario, u.foto_url
        ORDER BY u.nome
      `, [selectedActualGroupIds]);
      consultants = rows;
    } catch (e) {
      // Compatibilidade com bases antigas sem a coluna foto_url.
      if (String(e?.code) !== '42703') throw e;
      const { rows } = await db.query(`
        SELECT
          u.id,
          u.login,
          u.nome AS name,
          u.email,
          u.tipo_usuario AS user_type,
          NULL::text AS photo_url,
          u.id_zendesk AS requester_id,
          COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids,
          COALESCE(array_agg(DISTINCT g.id::text), '{}') AS group_ids,
          COALESCE(array_agg(DISTINCT g.nome), '{}') AS group_names
        FROM usuarios u
        JOIN membros_grupo mg ON mg.usuario_id = u.id
        JOIN grupos g ON g.id = mg.grupo_id
        LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
        WHERE mg.grupo_id = ANY($1::uuid[])
          AND u.id_zendesk IS NOT NULL
        GROUP BY u.id, u.login, u.nome, u.email, u.tipo_usuario
        ORDER BY u.nome
      `, [selectedActualGroupIds]);
      consultants = rows;
    }
  }

  return {
    groups,
    consultants,
    selectedGroupIds: selectedGroupIds.map(v => String(v)),
    selectedGroupNames: groups
      .filter(g => selectedGroupIds.includes(String(g.id)))
      .map(g => g.name),
  };
}

async function saveConsultantMonthlyCsat(monthRef, csatRanking, source, errorMsg) {
  if (!monthRef || !Array.isArray(csatRanking) || !csatRanking.length) return 0;

  const monthDate = `${monthRef}-01`;
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    for (const row of csatRanking) {
      await client.query(`
        INSERT INTO historico_csat_consultor (
          consultor_id,
          mes_ref,
          csat_percent,
          total_avaliacoes,
          positivos,
          negativos,
          fonte,
          erro,
          atualizado_em
        )
        VALUES ($1::uuid, $2::date, $3::numeric, $4::int, $5::int, $6::int, $7::varchar, $8::text, NOW())
        ON CONFLICT (consultor_id, mes_ref)
        DO UPDATE SET
          csat_percent = EXCLUDED.csat_percent,
          total_avaliacoes = EXCLUDED.total_avaliacoes,
          positivos = EXCLUDED.positivos,
          negativos = EXCLUDED.negativos,
          fonte = EXCLUDED.fonte,
          erro = EXCLUDED.erro,
          atualizado_em = NOW()
      `, [
        row.consultant_id,
        monthDate,
        row.score === null ? null : Number(row.score),
        Number(row.total || 0),
        Number(row.good || 0),
        Number(row.bad || 0),
        normalizeHistorySourceTag(source, 'zendesk'),
        errorMsg ? String(errorMsg) : null,
      ]);
    }
    await client.query('COMMIT');
    return csatRanking.length;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function saveConsultantMonthlyRanking(monthRef, cards) {
  if (!monthRef || !cards || typeof cards !== 'object') return 0;

  const monthDate = `${monthRef}-01`;
  const client = await db.connect();
  let saved = 0;
  try {
    await client.query('BEGIN');
    for (const metric of CONSULTANT_RANK_METRICS) {
      const card = cards[metric] || {};
      const ranking = Array.isArray(card.ranking) ? card.ranking : [];
      const source = normalizeHistorySourceTag(card.source || (
        metric === 'csat' || metric === 'total_atendimentos'
          ? 'zendesk'
          : (metric === 'avaliacoes_pares' ? 'google_sheets' : 'database')
      ), metric === 'avaliacoes_pares' ? 'google_sheets' : 'database');
      const errorMsg = card.error ? String(card.error) : null;

      for (const row of ranking) {
        if (!row.consultant_id) continue;
        await client.query(`
          INSERT INTO historico_ranking_consultores (
            consultor_id,
            mes_ref,
            metrica,
            score,
            total_registros,
            positivos,
            negativos,
            fonte,
            erro,
            atualizado_em
          )
          VALUES ($1::uuid, $2::date, $3::varchar, $4::numeric, $5::int, $6::int, $7::int, $8::varchar, $9::text, NOW())
          ON CONFLICT (consultor_id, mes_ref, metrica)
          DO UPDATE SET
            score = EXCLUDED.score,
            total_registros = EXCLUDED.total_registros,
            positivos = EXCLUDED.positivos,
            negativos = EXCLUDED.negativos,
            fonte = EXCLUDED.fonte,
            erro = EXCLUDED.erro,
            atualizado_em = NOW()
        `, [
          row.consultant_id,
          monthDate,
          metric,
          row.score === null || row.score === undefined ? null : Number(row.score),
          Number(row.total || 0),
          metric === 'csat' ? Number(row.good || 0) : null,
          metric === 'csat' ? Number(row.bad || 0) : null,
          source,
          errorMsg,
        ]);
        saved += 1;
      }
    }
    await client.query('COMMIT');
    return saved;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function saveConsultantMonthlyHistory(monthRef, consultants, cards) {
  if (!monthRef || !Array.isArray(consultants) || !consultants.length || !cards) return 0;

  const monthDate = `${monthRef}-01`;
  const csatMap = {};
  const atendimentoMap = {};
  const negativadosMap = {};
  const pdcaMap = {};
  const totalAtendimentosMap = {};
  const avaliacoesParesMap = {};

  (cards.csat?.ranking || []).forEach(r => { csatMap[String(r.consultant_id)] = r; });
  (cards.atendimento?.ranking || []).forEach(r => { atendimentoMap[String(r.consultant_id)] = r; });
  (cards.negativados?.ranking || []).forEach(r => { negativadosMap[String(r.consultant_id)] = r; });
  (cards.pdca?.ranking || []).forEach(r => { pdcaMap[String(r.consultant_id)] = r; });
  (cards.total_atendimentos?.ranking || []).forEach(r => { totalAtendimentosMap[String(r.consultant_id)] = r; });
  (cards.avaliacoes_pares?.ranking || []).forEach(r => { avaliacoesParesMap[String(r.consultant_id)] = r; });

  const client = await db.connect();
  try {
    await client.query('BEGIN');
    for (const c of consultants) {
      const cid = String(c.id);
      const csat = csatMap[cid] || {};
      const atendimento = atendimentoMap[cid] || {};
      const negativados = negativadosMap[cid] || {};
      const pdca = pdcaMap[cid] || {};
      const totalAtendimentos = totalAtendimentosMap[cid] || {};
      const avaliacoesPares = avaliacoesParesMap[cid] || {};

      await client.query(`
        INSERT INTO historico_csat_consultor (
          consultor_id,
          mes_ref,
          csat_percent,
          total_avaliacoes,
          positivos,
          negativos,
          fonte,
          erro,
          atendimento_percent,
          atendimento_total,
          negativados_percent,
          negativados_total,
          pdca_percent,
          pdca_total,
          total_atendimentos,
          fonte_total_atendimentos,
          erro_total_atendimentos,
          avaliacoes_pares_percent,
          avaliacoes_pares_total,
          avaliacoes_pares_par_percent,
          avaliacoes_pares_par_total,
          avaliacoes_pares_gestor_percent,
          avaliacoes_pares_gestor_total,
          fonte_avaliacoes_pares,
          erro_avaliacoes_pares,
          atualizado_em
        )
        VALUES (
          $1::uuid, $2::date, $3::numeric, $4::int, $5::int, $6::int, $7::varchar, $8::text,
          $9::numeric, $10::int, $11::numeric, $12::int, $13::numeric, $14::int, $15::int, $16::varchar, $17::text,
          $18::numeric, $19::int, $20::numeric, $21::int, $22::numeric, $23::int, $24::varchar, $25::text, NOW()
        )
        ON CONFLICT (consultor_id, mes_ref)
        DO UPDATE SET
          csat_percent = EXCLUDED.csat_percent,
          total_avaliacoes = EXCLUDED.total_avaliacoes,
          positivos = EXCLUDED.positivos,
          negativos = EXCLUDED.negativos,
          fonte = EXCLUDED.fonte,
          erro = EXCLUDED.erro,
          atendimento_percent = EXCLUDED.atendimento_percent,
          atendimento_total = EXCLUDED.atendimento_total,
          negativados_percent = EXCLUDED.negativados_percent,
          negativados_total = EXCLUDED.negativados_total,
          pdca_percent = EXCLUDED.pdca_percent,
          pdca_total = EXCLUDED.pdca_total,
          total_atendimentos = EXCLUDED.total_atendimentos,
          fonte_total_atendimentos = EXCLUDED.fonte_total_atendimentos,
          erro_total_atendimentos = EXCLUDED.erro_total_atendimentos,
          avaliacoes_pares_percent = EXCLUDED.avaliacoes_pares_percent,
          avaliacoes_pares_total = EXCLUDED.avaliacoes_pares_total,
          avaliacoes_pares_par_percent = EXCLUDED.avaliacoes_pares_par_percent,
          avaliacoes_pares_par_total = EXCLUDED.avaliacoes_pares_par_total,
          avaliacoes_pares_gestor_percent = EXCLUDED.avaliacoes_pares_gestor_percent,
          avaliacoes_pares_gestor_total = EXCLUDED.avaliacoes_pares_gestor_total,
          fonte_avaliacoes_pares = EXCLUDED.fonte_avaliacoes_pares,
          erro_avaliacoes_pares = EXCLUDED.erro_avaliacoes_pares,
          atualizado_em = NOW()
      `, [
        cid,
        monthDate,
        csat.score === null || csat.score === undefined ? null : Number(csat.score),
        Number(csat.total || 0),
        Number(csat.good || 0),
        Number(csat.bad || 0),
        normalizeHistorySourceTag(cards.csat?.source, 'zendesk'),
        cards.csat?.error ? String(cards.csat.error) : null,
        atendimento.score === null || atendimento.score === undefined ? null : Number(atendimento.score),
        Number(atendimento.total || 0),
        negativados.score === null || negativados.score === undefined ? null : Number(negativados.score),
        Number(negativados.total || 0),
        pdca.score === null || pdca.score === undefined ? null : Number(pdca.score),
        Number(pdca.total || 0),
        Number(totalAtendimentos.total || 0),
        normalizeHistorySourceTag(cards.total_atendimentos?.source, 'zendesk'),
        cards.total_atendimentos?.error ? String(cards.total_atendimentos.error) : null,
        avaliacoesPares.score === null || avaliacoesPares.score === undefined ? null : Number(avaliacoesPares.score),
        Number(avaliacoesPares.total || 0),
        avaliacoesPares.par_score === null || avaliacoesPares.par_score === undefined ? null : Number(avaliacoesPares.par_score),
        Number(avaliacoesPares.par_total || 0),
        avaliacoesPares.gestor_score === null || avaliacoesPares.gestor_score === undefined ? null : Number(avaliacoesPares.gestor_score),
        Number(avaliacoesPares.gestor_total || 0),
        normalizeHistorySourceTag(cards.avaliacoes_pares?.source, 'google_sheets'),
        cards.avaliacoes_pares?.error ? String(cards.avaliacoes_pares.error) : null,
      ]);
    }
    await client.query('COMMIT');
    return consultants.length;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  const loginInput = String(username || '').trim();
  if (!loginInput || !password) return res.status(400).json({ error: 'Usuario/e-mail e senha obrigatorios' });
  try {
    const { rows } = await db.query(
      `SELECT * FROM usuarios
       WHERE LOWER(login) = LOWER($1)
          OR LOWER(COALESCE(email, '')) = LOWER($1)
       LIMIT 1`,
      [loginInput]
    );
    const user = rows[0];
    if (!user) return res.status(401).json({ error: 'Usuario ou senha incorretos' });
    if (!await bcrypt.compare(password, user.senha_hash)) return res.status(401).json({ error: 'Usuario ou senha incorretos' });

    // Busca assignee_ids para incluir no token
    const { rows: aids } = await db.query(
      'SELECT id_zendesk FROM ids_atendente WHERE usuario_id = $1',
      [user.id]
    );
    const assignee_ids = aids.map(r => r.id_zendesk);

    const token = jwt.sign(
      { id:user.id, username:user.login, name:user.nome, role:user.papel, email:user.email, assignee_ids },
      JWT_SECRET, { expiresIn:'8h' }
    );
    res.json({ token, user:{ id:user.id, name:user.nome, username:user.login, role:user.papel, email:user.email, photo_url:user.foto_url || null } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/change-password', authenticate, async (req, res) => {
  const { currentPassword, newPassword } = req.body;
  if (!currentPassword || !newPassword || newPassword.length < 6) return res.status(400).json({ error: 'Dados invalidos' });
  try {
    const { rows } = await db.query('SELECT * FROM usuarios WHERE id = $1', [req.user.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Usuario nao encontrado' });
    if (!await bcrypt.compare(currentPassword, rows[0].senha_hash)) return res.status(401).json({ error: 'Senha atual incorreta' });
    const hash = await bcrypt.hash(newPassword, 10);
    await db.query('UPDATE usuarios SET senha_hash = $1, atualizado_em = NOW() WHERE id = $2', [hash, req.user.id]);
    res.json({ message: 'Senha alterada' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   GOOGLE OAUTH
══════════════════════════════════════════════════════════════ */

app.get('/api/auth/google', (req, res) => {
  const googleClientId = process.env.GOOGLE_CLIENT_ID;
  if (!googleClientId) {
    return res.redirect('/?error=' + encodeURIComponent('Google OAuth não configurado'));
  }
  
  const redirectUri = `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
  const scope = 'email profile';
  const state = Math.random().toString(36).substring(7);
  
  const googleAuthUrl = `https://accounts.google.com/o/oauth2/v2/auth?` +
    `client_id=${googleClientId}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&response_type=code` +
    `&scope=${encodeURIComponent(scope)}` +
    `&state=${state}` +
    `&access_type=offline` +
    `&prompt=select_account`;
  
  res.redirect(googleAuthUrl);
});

app.get('/api/auth/google/callback', async (req, res) => {
  const { code, error } = req.query;
  
  if (error) {
    return res.redirect('/?error=' + encodeURIComponent('Acesso negado pelo Google'));
  }
  
  if (!code) {
    return res.redirect('/?error=' + encodeURIComponent('Código de autorização não fornecido'));
  }
  
  try {
    const googleClientId = process.env.GOOGLE_CLIENT_ID;
    const googleClientSecret = process.env.GOOGLE_CLIENT_SECRET;
    
    if (!googleClientId || !googleClientSecret) {
      return res.redirect('/?error=' + encodeURIComponent('Google OAuth não configurado'));
    }
    
    const redirectUri = `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
    
    // Troca o código por access token
    const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: googleClientId,
        client_secret: googleClientSecret,
        redirect_uri: redirectUri,
        grant_type: 'authorization_code',
      }),
    });
    
    const tokenData = await tokenResponse.json();
    
    if (!tokenResponse.ok) {
      console.error('Google token error:', tokenData);
      return res.redirect('/?error=' + encodeURIComponent('Erro ao obter token do Google'));
    }
    
    // Busca informações do usuário
    const userResponse = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    
    const googleUser = await userResponse.json();
    
    if (!userResponse.ok) {
      console.error('Google user info error:', googleUser);
      return res.redirect('/?error=' + encodeURIComponent('Erro ao obter dados do usuário'));
    }
    
    // Busca ou cria usuário no banco
    let { rows } = await db.query(
      'SELECT * FROM usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1',
      [googleUser.email]
    );
    
    let user = rows[0];

    if (user && !user.foto_url && googleUser.picture) {
      const upd = await db.query(
        'UPDATE usuarios SET foto_url = $1, atualizado_em = NOW() WHERE id = $2 RETURNING *',
        [googleUser.picture, user.id]
      );
      user = upd.rows[0] || user;
    }
    
    if (!user) {
      // Cria novo usuário se não existir
      const username = googleUser.email.split('@')[0].toLowerCase().replace(/[^a-z0-9]/g, '.');
      const randomPass = await bcrypt.hash(Math.random().toString(36), 10);
      
      const insert = await db.query(`
        INSERT INTO usuarios (login, nome, email, senha_hash, papel, foto_url)
        VALUES ($1, $2, $3, $4, 'consultant', $5)
        ON CONFLICT (login) DO UPDATE SET
          email = EXCLUDED.email,
          foto_url = COALESCE(usuarios.foto_url, EXCLUDED.foto_url)
        RETURNING *
      `, [username, googleUser.name, googleUser.email, randomPass, googleUser.picture || null]);
      
      user = insert.rows[0];
    }
    
    // Busca assignee_ids
    const { rows: aids } = await db.query(
      'SELECT id_zendesk FROM ids_atendente WHERE usuario_id = $1',
      [user.id]
    );
    const assignee_ids = aids.map(r => r.id_zendesk);
    
    // Gera token JWT
    const token = jwt.sign(
      { id: user.id, username: user.login, name: user.nome, role: user.papel, email: user.email, assignee_ids },
      JWT_SECRET,
      { expiresIn: '8h' }
    );
    
    // Redireciona de volta para o frontend com o token
    res.redirect('/?token=' + token);
    
  } catch(e) {
    console.error('Google OAuth error:', e);
    res.redirect('/?error=' + encodeURIComponent('Erro ao autenticar: ' + e.message));
  }
});

app.get('/api/auth/validate', authenticate, async (req, res) => {
  try {
    const { rows } = await db.query(
      'SELECT id, login, nome, email, papel, foto_url FROM usuarios WHERE id = $1 LIMIT 1',
      [req.user.id]
    );
    const user = rows[0];
    if (!user) return res.status(404).json({ error: 'Usuario nao encontrado' });
    res.json({ user: { id: user.id, name: user.nome, username: user.login, role: user.papel, email: user.email, photo_url: user.foto_url || null } });
  } catch(e) {
    res.status(401).json({ error: 'Token inválido' });
  }
});

app.post('/api/me/photo', authenticate, async (req, res) => {
  const photoUrlRaw = req.body?.photo_url;
  if (photoUrlRaw === undefined) {
    return res.status(400).json({ error: 'Campo photo_url obrigatorio' });
  }
  const photoUrl = String(photoUrlRaw || '').trim();
  if (photoUrl) {
    const isHttp = /^https?:\/\//i.test(photoUrl);
    const isDataImage = /^data:image\//i.test(photoUrl);
    if (!isHttp && !isDataImage) {
      return res.status(400).json({ error: 'Foto invalida. Use URL http(s) ou data URL de imagem.' });
    }
    if (photoUrl.length > 2_000_000) {
      return res.status(400).json({ error: 'Imagem muito grande para salvar' });
    }
  }
  try {
    const { rows } = await db.query(`
      UPDATE usuarios
         SET foto_url = $1,
             atualizado_em = NOW()
       WHERE id = $2
      RETURNING id, login, nome, email, papel, foto_url
    `, [photoUrl || null, req.user.id]);
    const user = rows[0];
    if (!user) return res.status(404).json({ error: 'Usuario nao encontrado' });
    res.json({
      message: 'Foto atualizada',
      user: {
        id: user.id,
        name: user.nome,
        username: user.login,
        role: user.papel,
        email: user.email,
        photo_url: user.foto_url || null,
      }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ══════════════════════════════════════════════════════════════
   CONFIG
══════════════════════════════════════════════════════════════ */

// Rota pública para avaliadores verem os critérios de avaliação
app.get('/api/evaluation-criteria', authenticate, canEvaluate, async (req, res) => {
  try {
    const c = await getCfgForUser(req.user);
    res.json({ criteria: c.documentacao_base || '' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/config', authenticate, adminOnly, async (req, res) => {
  try {
    const c = await getCfg();
    return res.json(cfgToApiDto(c, { scope: 'global' }));
    res.json({
      zendeskSubdomain: c.zendesk_subdominio||'', zendeskEmail: c.zendesk_email||'', zendeskHasToken: !!c.zendesk_token,
      smtpHost: c.smtp_servidor||'', smtpPort: c.smtp_porta||587, smtpSecure: c.smtp_seguro||false,
      smtpUser: c.smtp_usuario||'', smtpHasPass: !!c.smtp_senha, smtpFromName: c.smtp_nome_remetente||'TecnoIT',
      anthropicHasKey: !!c.ia_chave_api, aiModel: c.ia_modelo||'gemini-2.0-flash', basicampDocs: c.documentacao_base||''
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/config/my', authenticate, async (req, res) => {
  if (req.user.role !== 'manager') {
    return res.status(403).json({ error: 'Acesso restrito ao responsavel de grupo' });
  }
  try {
    const globalCfg = await getCfg();
    const scopedCfg = await getScopedCfgByUserId(req.user.id);
    const effectiveCfg = mergeCfg(globalCfg, scopedCfg);
    res.json(cfgToApiDto(effectiveCfg, {
      scope: 'manager',
      hasOwnConfig: !!scopedCfg,
    }));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/admin/config', authenticate, adminOnly, async (req, res) => {
  const b = req.body;
  try {
    await db.query(`
      UPDATE configuracoes SET
        zendesk_subdominio  = COALESCE($1::varchar, zendesk_subdominio),
        zendesk_email       = COALESCE($2::varchar, zendesk_email),
        zendesk_token       = CASE WHEN $3::varchar IS NOT NULL AND $3 <> '' THEN $3 ELSE zendesk_token END,
        smtp_servidor       = COALESCE($4::varchar, smtp_servidor),
        smtp_porta          = COALESCE($5::integer, smtp_porta),
        smtp_seguro         = COALESCE($6::boolean, smtp_seguro),
        smtp_usuario        = COALESCE($7::varchar, smtp_usuario),
        smtp_senha          = CASE WHEN $8::varchar IS NOT NULL AND $8 <> '' THEN $8 ELSE smtp_senha END,
        smtp_nome_remetente = COALESCE($9::varchar, smtp_nome_remetente),
        ia_chave_api        = CASE WHEN $10::varchar IS NOT NULL AND $10 <> '' THEN $10 ELSE ia_chave_api END,
        ia_modelo           = COALESCE($11::varchar, ia_modelo),
        documentacao_base   = COALESCE($12::text, documentacao_base),
        atualizado_em       = NOW()
    `, [
      b.zendeskSubdomain !== undefined ? b.zendeskSubdomain.trim() : null,
      b.zendeskEmail     !== undefined ? b.zendeskEmail.trim()     : null,
      b.zendeskToken?.trim() || null,
      b.smtpHost     !== undefined ? b.smtpHost.trim()     : null,
      b.smtpPort     !== undefined ? Number(b.smtpPort)    : null,
      b.smtpSecure   !== undefined ? !!b.smtpSecure        : null,
      b.smtpUser     !== undefined ? b.smtpUser.trim()     : null,
      b.smtpPass?.trim() || null,
      b.smtpFromName !== undefined ? b.smtpFromName.trim() : null,
      b.anthropicKey?.trim() || null,
      b.aiModel      !== undefined ? b.aiModel.trim()      : null,
      b.basicampDocs !== undefined ? b.basicampDocs        : null,
    ]);
    res.json({ message: 'Configuracao salva' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   USUÁRIOS (admin)
══════════════════════════════════════════════════════════════ */

app.post('/api/config/my', authenticate, async (req, res) => {
  if (req.user.role !== 'manager') {
    return res.status(403).json({ error: 'Acesso restrito ao responsavel de grupo' });
  }
  const b = req.body || {};
  try {
    await db.query(`
      INSERT INTO configuracoes_responsavel (
        usuario_id,
        zendesk_subdominio,
        zendesk_email,
        zendesk_token,
        smtp_servidor,
        smtp_porta,
        smtp_seguro,
        smtp_usuario,
        smtp_senha,
        smtp_nome_remetente,
        ia_chave_api,
        ia_modelo,
        documentacao_base,
        atualizado_em
      )
      VALUES (
        $1::uuid, $2::varchar, $3::varchar, $4::varchar, $5::varchar, $6::integer, $7::boolean, $8::varchar, $9::varchar, $10::varchar, $11::varchar, $12::varchar, $13::text, NOW()
      )
      ON CONFLICT (usuario_id)
      DO UPDATE SET
        zendesk_subdominio  = COALESCE($2::varchar, configuracoes_responsavel.zendesk_subdominio),
        zendesk_email       = COALESCE($3::varchar, configuracoes_responsavel.zendesk_email),
        zendesk_token       = CASE WHEN $4::varchar IS NOT NULL AND $4 <> '' THEN $4 ELSE configuracoes_responsavel.zendesk_token END,
        smtp_servidor       = COALESCE($5::varchar, configuracoes_responsavel.smtp_servidor),
        smtp_porta          = COALESCE($6::integer, configuracoes_responsavel.smtp_porta),
        smtp_seguro         = COALESCE($7::boolean, configuracoes_responsavel.smtp_seguro),
        smtp_usuario        = COALESCE($8::varchar, configuracoes_responsavel.smtp_usuario),
        smtp_senha          = CASE WHEN $9::varchar IS NOT NULL AND $9 <> '' THEN $9 ELSE configuracoes_responsavel.smtp_senha END,
        smtp_nome_remetente = COALESCE($10::varchar, configuracoes_responsavel.smtp_nome_remetente),
        ia_chave_api        = CASE WHEN $11::varchar IS NOT NULL AND $11 <> '' THEN $11 ELSE configuracoes_responsavel.ia_chave_api END,
        ia_modelo           = COALESCE($12::varchar, configuracoes_responsavel.ia_modelo),
        documentacao_base   = COALESCE($13::text, configuracoes_responsavel.documentacao_base),
        atualizado_em       = NOW()
    `, [
      req.user.id,
      b.zendeskSubdomain !== undefined ? b.zendeskSubdomain.trim() : null,
      b.zendeskEmail     !== undefined ? b.zendeskEmail.trim()     : null,
      b.zendeskToken?.trim() || null,
      b.smtpHost     !== undefined ? b.smtpHost.trim()     : null,
      b.smtpPort     !== undefined ? Number(b.smtpPort)    : null,
      b.smtpSecure   !== undefined ? !!b.smtpSecure        : null,
      b.smtpUser     !== undefined ? b.smtpUser.trim()     : null,
      b.smtpPass?.trim() || null,
      b.smtpFromName !== undefined ? b.smtpFromName.trim() : null,
      b.anthropicKey?.trim() || null,
      b.aiModel      !== undefined ? b.aiModel.trim()      : null,
      b.basicampDocs !== undefined ? b.basicampDocs        : null,
    ]);
    res.json({ message: 'Configuracao do responsavel salva' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/users', authenticate, adminOrManagerOnly, async (req, res) => {
  try {
    const isAdmin = req.user.role === 'admin';
    const params = [];
    let whereSql = '';
    if (!isAdmin) {
      const operableGroupIds = await getManagedActualGroupIds(req.user.id);
      params.push(req.user.id);
      params.push(operableGroupIds);
      whereSql = `
        WHERE (
          u.id = $1::uuid
          OR u.id IN (
            SELECT DISTINCT mg.usuario_id
            FROM membros_grupo mg
            WHERE mg.grupo_id = ANY($2::uuid[])
          )
        )
      `;
    }

    const { rows } = await db.query(`
      SELECT u.id, u.login AS username, u.nome AS name, u.email, u.papel AS role,
             u.tipo_usuario AS user_type,
             u.id_zendesk AS zendesk_agent_id,
             COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids,
             COALESCE(array_agg(DISTINCT mg.grupo_id)   FILTER (WHERE mg.grupo_id IS NOT NULL),   '{}') AS group_ids,
             COALESCE(array_agg(DISTINCT g.nome)        FILTER (WHERE g.nome IS NOT NULL),        '{}') AS zendesk_groups
      FROM usuarios u
      LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
      LEFT JOIN membros_grupo mg ON mg.usuario_id = u.id
      LEFT JOIN grupos g         ON g.id = mg.grupo_id
      ${whereSql}
      GROUP BY u.id
      ORDER BY u.nome
    `, params);
    res.json({ users: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users', authenticate, adminOrManagerOnly, async (req, res) => {
  const body = req.body || {};
  const name = String(body.name || '').replace(/\s+/g, ' ').trim();
  const email = normalizeEmail(body.email || '');
  const requestedGroupIds = dedupeStringIds(body.group_ids);
  const zendeskAgentId = String(body.zendesk_agent_id || '').trim() || null;
  const userType = normalizeUserType(body.user_type);

  if (!name) return res.status(400).json({ error: 'Informe o nome do usuario' });
  if (body.user_type !== undefined && body.user_type !== null && !userType) {
    return res.status(400).json({ error: 'Tipo de usuario invalido' });
  }

  try {
    const groupIds = await ensureManageableGroupIds(req.user, requestedGroupIds);
    if (req.user.role === 'manager' && !groupIds.length) {
      return res.status(400).json({ error: 'Selecione ao menos um grupo para o usuario' });
    }

    let assignmentGroupIds = groupIds;
    if (groupIds.length) {
      const groupDefinitions = await fetchGroupDefinitions(groupIds);
      const foundIds = new Set(groupDefinitions.map(def => String(def.id)));
      const invalidGroup = groupIds.find(id => !foundIds.has(String(id)));
      if (invalidGroup) return res.status(400).json({ error: 'Grupo informado nao existe' });
      const definitionMap = new Map(groupDefinitions.map(def => [String(def.id), def]));
      assignmentGroupIds = dedupeStringIds(
        groupDefinitions.flatMap(def => resolveDefinitionActualGroupIds(def, definitionMap))
      );
      if (!assignmentGroupIds.length) {
        return res.status(400).json({ error: 'O agrupador selecionado nao possui grupos Zendesk vinculados' });
      }
    }

    if (email) {
      const { rows: emailRows } = await db.query(
        'SELECT id FROM usuarios WHERE LOWER(email) = LOWER($1) LIMIT 1',
        [email]
      );
      if (emailRows.length) return res.status(409).json({ error: 'Ja existe um usuario com este e-mail' });
    }

    if (zendeskAgentId) {
      const { rows: agentRows } = await db.query(`
        SELECT id FROM usuarios WHERE id_zendesk = $1
        UNION
        SELECT usuario_id AS id FROM ids_atendente WHERE id_zendesk = $1
        LIMIT 1
      `, [zendeskAgentId]);
      if (agentRows.length) return res.status(409).json({ error: 'Ja existe um usuario com este ID Zendesk' });
    }

    const loginSeed = String(body.login || '').trim() || email.split('@')[0] || name;
    const login = await getNextAvailableLogin(loginSeed);
    const passwordHash = await bcrypt.hash('senha123', 10);

    const { rows } = await db.query(`
      INSERT INTO usuarios (login, nome, email, senha_hash, papel, tipo_usuario, id_zendesk)
      VALUES ($1, $2, $3, $4, 'consultant', $5, $6)
      RETURNING id, login AS username, nome AS name, email, papel AS role, tipo_usuario AS user_type, id_zendesk AS zendesk_agent_id
    `, [login, name, email || '', passwordHash, userType, zendeskAgentId]);

    const createdUser = rows[0];
    if (zendeskAgentId) {
      await db.query(`
        INSERT INTO ids_atendente (usuario_id, id_zendesk)
        VALUES ($1::uuid, $2)
        ON CONFLICT DO NOTHING
      `, [createdUser.id, zendeskAgentId]);
    }

    for (const groupId of assignmentGroupIds) {
      await db.query(`
        INSERT INTO membros_grupo (usuario_id, grupo_id)
        VALUES ($1::uuid, $2::uuid)
        ON CONFLICT DO NOTHING
      `, [createdUser.id, groupId]);
    }

    res.status(201).json({
      message: 'Usuario criado com sucesso',
      user: {
        ...createdUser,
        assignee_ids: zendeskAgentId ? [zendeskAgentId] : [],
        group_ids: assignmentGroupIds,
      },
    });
  } catch(e) {
    if (e.code === '23505') {
      return res.status(409).json({ error: 'Nao foi possivel criar o usuario por conflito de login, grupo ou Zendesk ID' });
    }
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.post('/api/admin/users/:id/email', authenticate, adminOrManagerOnly, async (req, res) => {
  try {
    await ensureManageableUser(req.user, req.params.id);
    await db.query('UPDATE usuarios SET email = $1, atualizado_em = NOW() WHERE id = $2', [req.body.email || '', req.params.id]);
    res.json({ message: 'E-mail atualizado' });
  } catch(e) { res.status(e.statusCode || 500).json({ error: e.message }); }
});

app.post('/api/admin/users/:id/role', authenticate, adminOnly, async (req, res) => {
  const { role } = req.body;
  if (!['consultant','evaluator','admin','manager'].includes(role)) return res.status(400).json({ error: 'Role invalido' });
  try {
    const { rowCount } = await db.query(
      'UPDATE usuarios SET papel = $1, atualizado_em = NOW() WHERE id = $2',
      [role, req.params.id]
    );
    if (!rowCount) return res.status(404).json({ error: 'Usuario nao encontrado' });
    res.json({ message: 'Role atualizado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users/:id/user-type', authenticate, adminOrManagerOnly, async (req, res) => {
  const requestedUserType = req.body?.user_type;
  const userType = normalizeUserType(requestedUserType);
  if (requestedUserType !== undefined && requestedUserType !== null && requestedUserType !== '' && !userType) {
    return res.status(400).json({ error: 'Tipo de usuario invalido' });
  }
  try {
    await ensureManageableUser(req.user, req.params.id);
    const { rowCount } = await db.query(
      'UPDATE usuarios SET tipo_usuario = $1, atualizado_em = NOW() WHERE id = $2',
      [userType, req.params.id]
    );
    if (!rowCount) return res.status(404).json({ error: 'Usuario nao encontrado' });
    res.json({ message: 'Tipo de usuario atualizado', user_type: userType });
  } catch(e) { res.status(e.statusCode || 500).json({ error: e.message }); }
});

app.delete('/api/admin/users/:id', authenticate, adminOnly, async (req, res) => {
  const userId = String(req.params.id || '').trim();
  if (!userId) return res.status(400).json({ error: 'Usuario invalido' });
  if (String(req.user.id) === userId) {
    return res.status(400).json({ error: 'Voce nao pode excluir o proprio usuario logado' });
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      'SELECT id, login, nome, papel AS role FROM usuarios WHERE id = $1::uuid LIMIT 1',
      [userId]
    );
    const targetUser = rows[0];
    if (!targetUser) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Usuario nao encontrado' });
    }
    if (String(targetUser.role) === 'admin') {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Nao e permitido excluir um administrador' });
    }

    await client.query('UPDATE tickets SET consultor_id = NULL WHERE consultor_id = $1::uuid', [userId]);
    await client.query('UPDATE pdca_tickets SET consultor_id = NULL WHERE consultor_id = $1::uuid', [userId]);
    await client.query('UPDATE avaliacoes SET avaliador_id = NULL WHERE avaliador_id = $1::uuid', [userId]);
    await client.query('UPDATE historico_reavaliacoes SET avaliador_id = NULL WHERE avaliador_id = $1::uuid', [userId]);
    await client.query('UPDATE tickets_descartados SET consultor_id = NULL WHERE consultor_id = $1::uuid', [userId]).catch(() => {});

    const { rowCount } = await client.query('DELETE FROM usuarios WHERE id = $1::uuid', [userId]);
    if (!rowCount) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Usuario nao encontrado' });
    }
    await client.query('COMMIT');
    res.json({ message: 'Usuario excluido com sucesso', user: { id: targetUser.id, name: targetUser.nome || targetUser.name || '' } });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

/* ══════════════════════════════════════════════════════════════
   SYNC AGENTS FROM ZENDESK
══════════════════════════════════════════════════════════════ */

app.post('/api/admin/sync-agents-legacy', authenticate, adminOnly, async (req, res) => {
  const cfg = await getCfg();
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  try {
    // 1. Busca departamentos habilitados (enabled_departments) da API do Chat
    const deptsData = await zendeskChatRequest('/api/v2/chat/departments', cfg).catch(() => ({ departments: [] }));
    const enabledDepts = (deptsData.departments || []).filter(d => d.status === 'enabled');
    
    // Cria/atualiza grupos a partir dos departamentos habilitados
    const deptMap = {}; // zendesk_dept_id → name
    for (const dept of enabledDepts) {
      deptMap[String(dept.id)] = dept.name;
      await db.query(`
        INSERT INTO grupos (id_zendesk, nome, sincronizado_em)
        VALUES ($1, $2, NOW())
        ON CONFLICT (id_zendesk) DO UPDATE SET nome = EXCLUDED.nome, sincronizado_em = NOW()
      `, [String(dept.id), dept.name]);
    }

    // 2. Busca agentes da API do Chat
    const agentsData = await zendeskChatRequest('/api/v2/chat/agents', cfg).catch(() => ({ agents: [] }));
    const allAgents = agentsData.agents || [];
    
    // Mapeia agente → departamentos
    const agentDeptMap = {}; // zendesk_agent_id → [zendesk_dept_id, ...]
    for (const agent of allAgents) {
      if (!agent.enabled) continue;
      const agentId = String(agent.id);
      const depts = (agent.departments || []).map(d => String(d));
      agentDeptMap[agentId] = depts.filter(d => deptMap[d]); // Apenas departamentos habilitados
    }

    const dh = await bcrypt.hash('senha123', 10); // Senha padrão para todos os agentes sincronizados
    const normName  = s => (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim();
    const normEmail = s => (s||'').toLowerCase().trim();

    const resolveDepts = (agentId) => {
      const dids = agentDeptMap[String(agentId)] || [];
      return dids.map(did => deptMap[did]).filter(Boolean);
    };

    let created = 0, updated = 0, linked = 0;

    for (const agent of allAgents) {
      if (!agent.enabled) continue;
      const agentId    = String(agent.id);
      const depts      = resolveDepts(agentId);
      const username   = normName(agent.display_name || agent.first_name + ' ' + agent.last_name).replace(/\s+/g,'.');
      const agentEmail = normEmail(agent.email || '');
      const fullName   = agent.display_name || `${agent.first_name || ''} ${agent.last_name || ''}`.trim();

      // Tenta encontrar usuário existente
      let { rows } = await db.query(`
        SELECT u.id, u.id_zendesk FROM usuarios u
        WHERE u.id_zendesk = $1
           OR LOWER(u.nome) = LOWER($2)
           OR (u.email <> '' AND LOWER(u.email) = $3)
        LIMIT 1
      `, [agentId, fullName, agentEmail]);

      // Fallback: busca por assignee_id
      if (!rows.length) {
        const r2 = await db.query(
          'SELECT usuario_id AS id FROM ids_atendente WHERE id_zendesk = $1 LIMIT 1',
          [agentId]
        );
        if (r2.rows.length) rows = [{ id: r2.rows[0].id }];
      }

      let userId;
      if (rows.length) {
        userId = rows[0].id;
        const wasLinked = !rows[0].id_zendesk;
        await db.query(`
          UPDATE usuarios SET 
            nome = $1, 
            id_zendesk = $2::varchar,
            email = CASE WHEN (email IS NULL OR email = '') THEN $3::varchar ELSE email END,
            senha_hash = CASE WHEN senha_hash IS NULL THEN $5 ELSE senha_hash END,
            atualizado_em = NOW()
          WHERE id = $4
        `, [fullName, agentId, agentEmail, userId, dh]);
        wasLinked ? linked++ : updated++;
      } else {
        const ins = await db.query(`
          INSERT INTO usuarios (login, nome, email, senha_hash, papel, id_zendesk)
          VALUES ($1, $2, $3, $4, 'consultant', $5)
          ON CONFLICT (login) DO UPDATE SET nome = EXCLUDED.nome RETURNING id
        `, [username, fullName, agentEmail, dh, agentId]);
        userId = ins.rows[0].id;
        created++;
      }

      // Vincula ids_atendente
      await db.query(`
        INSERT INTO ids_atendente (usuario_id, id_zendesk)
        VALUES ($1, $2) ON CONFLICT DO NOTHING
      `, [userId, agentId]);

      // Atualiza membros_grupo com base nos departamentos
      await db.query('DELETE FROM membros_grupo WHERE usuario_id = $1', [userId]);
      for (const deptName of depts) {
        const { rows: gRows } = await db.query('SELECT id FROM grupos WHERE nome = $1 LIMIT 1', [deptName]);
        if (gRows.length) {
          await db.query(`
            INSERT INTO membros_grupo (usuario_id, grupo_id) VALUES ($1, $2) ON CONFLICT DO NOTHING
          `, [userId, gRows[0].id]);
        }
      }
    }

    res.json({
      message: 'Agentes sincronizados com sucesso (API Chat + Departamentos)',
      created,
      updated,
      linked,
      total: allAgents.filter(a => a.enabled).length,
      departments: enabledDepts.length
    });
  } catch (e) {
    console.error('[SYNC ERROR]', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ══════════════════════════════════════════════════════════════
   GRUPOS
══════════════════════════════════════════════════════════════ */

// Sync v2: tries Chat API first and falls back to Support API users/groups.
app.post('/api/admin/sync-agents', authenticate, adminOnly, async (req, res) => {
  const cfg = await getCfg();
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) {
    return res.status(400).json({ error: 'Zendesk nao configurado' });
  }
  try {
    const warnings = [];
    const deptMap = {};
    const agentDeptMap = {};
    let source = 'chat';
    let allAgents = [];

    const upsertGroup = async (groupId, groupName) => {
      const gid = String(groupId || '').trim();
      const gname = String(groupName || '').trim();
      if (!gid || !gname) return;
      deptMap[gid] = gname;
      await db.query(`
        INSERT INTO grupos (id_zendesk, nome, sincronizado_em)
        VALUES ($1, $2, NOW())
        ON CONFLICT (id_zendesk) DO UPDATE SET nome = EXCLUDED.nome, sincronizado_em = NOW()
      `, [gid, gname]);
    };

    try {
      const deptsData = await zendeskChatRequest('/api/v2/chat/departments', cfg);
      const enabledDepts = (deptsData.departments || []).filter((d) => {
        if (!d || d.id === undefined || d.id === null) return false;
        if (d.status === undefined || d.status === null) return true;
        return String(d.status).toLowerCase() === 'enabled';
      });
      for (const dept of enabledDepts) {
        await upsertGroup(dept.id, dept.name);
      }

      const agentsData = await zendeskChatRequest('/api/v2/chat/agents', cfg);
      allAgents = (agentsData.agents || []).filter(isZendeskAgentEnabled);
      allAgents.forEach((agent) => {
        const agentId = String(agent.id || '').trim();
        if (!agentId) return;
        const groupIds = extractZendeskAgentGroupIds(agent)
          .filter((gid) => deptMap[String(gid)]);
        agentDeptMap[agentId] = [...new Set(groupIds)];
      });
      if (!allAgents.length) {
        throw new Error('Nenhum agente retornado pela API de Chat');
      }
    } catch (chatErr) {
      source = 'support_fallback';
      warnings.push(`Chat API sem dados utilizaveis: ${String(chatErr?.message || 'erro desconhecido')}`);

      const supportGroups = await fetchZendeskSupportGroups(cfg);
      for (const g of supportGroups) {
        await upsertGroup(g.id, g.name);
      }

      let membershipsByUser = {};
      try {
        membershipsByUser = await fetchZendeskSupportGroupMembershipMap(cfg);
      } catch (membershipErr) {
        warnings.push(`Nao foi possivel carregar group_memberships: ${String(membershipErr?.message || 'erro desconhecido')}`);
      }

      allAgents = (await fetchZendeskSupportAgents(cfg)).filter(isZendeskAgentEnabled);
      allAgents.forEach((agent) => {
        const agentId = String(agent.id || '').trim();
        if (!agentId) return;
        const byMembership = membershipsByUser[agentId] || [];
        const byAgentPayload = extractZendeskAgentGroupIds(agent);
        const merged = [...new Set([...byMembership, ...byAgentPayload])]
          .filter((gid) => deptMap[String(gid)]);
        agentDeptMap[agentId] = merged;
      });
    }

    const dh = await bcrypt.hash('senha123', 10);
    const normName = (s) => (s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
    const normEmail = (s) => (s || '').toLowerCase().trim();
    const toLoginSlug = (s) => normName(s)
      .replace(/[^a-z0-9.\s_-]/g, '')
      .replace(/[ _-]+/g, '.')
      .replace(/\.+/g, '.')
      .replace(/^\.|\.$/g, '');
    const cleanupHumanName = (s) => String(s || '').replace(/\s+/g, ' ').trim();
    const prettyNameFromEmail = (email) => {
      const local = String(email || '').split('@')[0] || '';
      const parts = local
        .replace(/[._-]+/g, ' ')
        .split(' ')
        .map((p) => p.trim())
        .filter(Boolean)
        .map((p) => p.charAt(0).toUpperCase() + p.slice(1));
      return cleanupHumanName(parts.join(' '));
    };
    const isGenericAgentName = (name, agentId) => {
      const n = cleanupHumanName(name);
      if (!n) return true;
      const low = n.toLowerCase();
      if (agentId && (low === `agente ${String(agentId).toLowerCase()}` || low === `agent ${String(agentId).toLowerCase()}`)) return true;
      if (/^(agente|agent)\s+\d+$/i.test(n)) return true;
      if (/^\d+$/.test(n)) return true;
      return false;
    };
    const buildBestAgentName = async (agent) => {
      const agentId = String(agent?.id || '').trim();
      const email = normEmail(agent?.email || '');
      const candidates = [
        agent?.display_name,
        agent?.name,
        agent?.full_name,
        `${agent?.first_name || ''} ${agent?.last_name || ''}`.trim(),
        agent?.alias,
      ];
      let best = cleanupHumanName(candidates.find((v) => cleanupHumanName(v)) || '');

      if (isGenericAgentName(best, agentId) && agentId) {
        try {
          const detail = await zendeskRequest(`/api/v2/users/${encodeURIComponent(agentId)}.json`, cfg, 'SYNC_AGENT_DETAIL');
          const user = detail?.user || {};
          const detailName = cleanupHumanName(
            user.name
            || user.display_name
            || user.alias
            || `${user.first_name || ''} ${user.last_name || ''}`.trim()
          );
          if (detailName && !isGenericAgentName(detailName, agentId)) {
            best = detailName;
          }
        } catch (_) {
          // segue com fallback local
        }
      }

      if (isGenericAgentName(best, agentId) && email) {
        const fromEmail = prettyNameFromEmail(email);
        if (fromEmail) best = fromEmail;
      }

      if (!cleanupHumanName(best)) {
        best = 'Consultor sem nome';
      }
      return cleanupHumanName(best);
    };
    const nextAvailableLogin = async (seed, skipUserId = null) => {
      const base = toLoginSlug(seed) || 'consultor';
      let candidate = base;
      let idx = 1;
      while (idx < 1000) {
        const { rows: loginRows } = await db.query(
          'SELECT id FROM usuarios WHERE LOWER(login) = LOWER($1) LIMIT 1',
          [candidate]
        );
        if (!loginRows.length || (skipUserId && String(loginRows[0].id) === String(skipUserId))) {
          return candidate;
        }
        idx += 1;
        candidate = `${base}.${idx}`;
      }
      return `${base}.${Date.now()}`;
    };
    const resolveGroups = (agentId) => {
      const dids = agentDeptMap[String(agentId)] || [];
      return dids.map((did) => deptMap[String(did)]).filter(Boolean);
    };

    let created = 0;
    let updated = 0;
    let linked = 0;
    const syncedAgents = [];

    for (const agent of allAgents) {
      if (!isZendeskAgentEnabled(agent)) continue;
      const agentId = String(agent.id);
      const groups = resolveGroups(agentId);
      const agentEmail = normEmail(agent.email || '');
      const fullName = await buildBestAgentName(agent);
      const usernameFromName = toLoginSlug(fullName);
      const usernameFromEmail = toLoginSlug((agentEmail.split('@')[0] || '').replace(/[._-]+/g, ' '));
      const username = usernameFromName || usernameFromEmail || `zendesk.${agentId}`;

      let { rows } = await db.query(`
        SELECT u.id, u.id_zendesk, u.login FROM usuarios u
        WHERE u.id_zendesk = $1
           OR LOWER(u.nome) = LOWER($2)
           OR (u.email <> '' AND LOWER(u.email) = $3)
        LIMIT 1
      `, [agentId, fullName, agentEmail]);

      if (!rows.length) {
        const r2 = await db.query(
          'SELECT u.id, u.login FROM ids_atendente ia JOIN usuarios u ON u.id = ia.usuario_id WHERE ia.id_zendesk = $1 LIMIT 1',
          [agentId]
        );
        if (r2.rows.length) rows = [{ id: r2.rows[0].id, login: r2.rows[0].login, id_zendesk: null }];
      }

      let userId;
      let persistedLogin = username;
      if (rows.length) {
        userId = rows[0].id;
        const wasLinked = !rows[0].id_zendesk;
        const currentLogin = String(rows[0].login || '').trim();
        const replaceGenericLogin = !currentLogin || /^(agente|agent|zendesk)[._-]?\d+$/i.test(currentLogin);
        const loginForUpdate = replaceGenericLogin
          ? await nextAvailableLogin(username, userId)
          : null;
        persistedLogin = loginForUpdate || currentLogin || username;
        await db.query(`
          UPDATE usuarios SET
            nome = $1,
            id_zendesk = $2::varchar,
            email = CASE WHEN (email IS NULL OR email = '') THEN $3::varchar ELSE email END,
            senha_hash = CASE WHEN senha_hash IS NULL THEN $5 ELSE senha_hash END,
            login = COALESCE($6::varchar, login),
            atualizado_em = NOW()
          WHERE id = $4
        `, [fullName, agentId, agentEmail, userId, dh, loginForUpdate]);
        if (wasLinked) linked++;
        else updated++;
      } else {
        const insertLogin = await nextAvailableLogin(username);
        persistedLogin = insertLogin;
        const ins = await db.query(`
          INSERT INTO usuarios (login, nome, email, senha_hash, papel, id_zendesk)
          VALUES ($1, $2, $3, $4, 'consultant', $5)
          ON CONFLICT (login) DO UPDATE SET nome = EXCLUDED.nome RETURNING id
        `, [insertLogin, fullName, agentEmail, dh, agentId]);
        userId = ins.rows[0].id;
        created++;
      }

      await db.query(`
        INSERT INTO ids_atendente (usuario_id, id_zendesk)
        VALUES ($1, $2) ON CONFLICT DO NOTHING
      `, [userId, agentId]);

      await db.query('DELETE FROM membros_grupo WHERE usuario_id = $1', [userId]);
      for (const groupName of groups) {
        const { rows: gRows } = await db.query('SELECT id FROM grupos WHERE nome = $1 LIMIT 1', [groupName]);
        if (!gRows.length) continue;
        await db.query(`
          INSERT INTO membros_grupo (usuario_id, grupo_id)
          VALUES ($1, $2) ON CONFLICT DO NOTHING
        `, [userId, gRows[0].id]);
      }

      syncedAgents.push({
        id_zendesk: agentId,
        nome: fullName,
        login: persistedLogin,
        email: agentEmail || null,
        grupos: groups,
      });
    }

    return res.json({
      message: 'Agentes sincronizados com sucesso',
      source,
      warnings,
      created,
      updated,
      linked,
      total: allAgents.filter(isZendeskAgentEnabled).length,
      departments: Object.keys(deptMap).length,
      agents: syncedAgents,
    });
  } catch (e) {
    console.error('[SYNC ERROR V2]', e.message);
    return res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/groups', authenticate, adminOrManagerOnly, async (req, res) => {
  try {
    const definitions = await fetchGroupDefinitions();
    const definitionMap = new Map(definitions.map(def => [String(def.id), def]));
    const membershipRows = await fetchConsultantsByActualGroupIds(
      definitions
        .filter(def => def.kind !== 'aggregator')
        .map(def => def.id)
    );
    const consultantsByActualGroup = {};
    membershipRows.forEach((row) => {
      const groupId = String(row.actual_group_id || '');
      if (!consultantsByActualGroup[groupId]) consultantsByActualGroup[groupId] = [];
      consultantsByActualGroup[groupId].push({
        id: String(row.consultant_id),
        name: row.consultant_name,
        id_zendesk: row.requester_id ? String(row.requester_id) : null,
      });
    });

    const { rows: managerRows } = await db.query(`
      SELECT grupo_id::text AS group_id, usuario_id::text AS user_id
      FROM responsaveis_grupo
    `);
    const managerIdsByGroup = {};
    managerRows.forEach((row) => {
      const groupId = String(row.group_id || '');
      if (!managerIdsByGroup[groupId]) managerIdsByGroup[groupId] = [];
      managerIdsByGroup[groupId].push(String(row.user_id));
    });

    const groups = definitions.map((def) => {
      const resolvedGroupIds = resolveDefinitionActualGroupIds(def, definitionMap);
      const consultantsMap = new Map();
      resolvedGroupIds.forEach((groupId) => {
        (consultantsByActualGroup[groupId] || []).forEach((consultant) => {
          consultantsMap.set(String(consultant.id), consultant);
        });
      });
      return {
        id: String(def.id),
        id_zendesk: def.id_zendesk || null,
        name: def.name,
        kind: def.kind,
        linked_group_ids: def.linked_group_ids || [],
        linked_group_names: def.linked_group_names || [],
        resolved_group_ids: resolvedGroupIds,
        consultants: [...consultantsMap.values()],
        consultants_count: consultantsMap.size,
        manager_ids: managerIdsByGroup[String(def.id)] || [],
      };
    });

    res.json({ groups });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/groups', authenticate, adminOrManagerOnly, async (req, res) => {
  const body = req.body || {};
  const name = String(body.name || '').replace(/\s+/g, ' ').trim();
  const providedZendeskId = String(body.id_zendesk || '').trim();
  const linkedGroupIds = dedupeStringIds(body.linked_group_ids);
  const zendeskId = linkedGroupIds.length ? `manual:${uuidv4()}` : (providedZendeskId || `manual:${uuidv4()}`);

  if (!name) return res.status(400).json({ error: 'Informe o nome do grupo' });

  try {
    if (linkedGroupIds.length) {
      const childDefinitions = await fetchGroupDefinitions(linkedGroupIds);
      const foundIds = new Set(childDefinitions.map(def => String(def.id)));
      const invalidChild = linkedGroupIds.find(id => !foundIds.has(String(id)));
      if (invalidChild) return res.status(400).json({ error: 'Um dos grupos vinculados nao existe' });
      const invalidAggregator = childDefinitions.find(def => def.kind === 'aggregator');
      if (invalidAggregator) {
        return res.status(400).json({ error: 'Agrupadores nao podem ser vinculados dentro de outro agrupador' });
      }
    }

    const { rows } = await db.query(`
      INSERT INTO grupos (id_zendesk, nome, sincronizado_em)
      VALUES ($1, $2, NOW())
      RETURNING id, id_zendesk, nome AS name
    `, [zendeskId, name]);

    const group = rows[0];
    for (const linkedGroupId of linkedGroupIds) {
      await db.query(`
        INSERT INTO grupo_agrupador_vinculos (agrupador_id, grupo_id)
        VALUES ($1::uuid, $2::uuid)
        ON CONFLICT DO NOTHING
      `, [group.id, linkedGroupId]);
    }
    if (req.user.role === 'manager') {
      await db.query(`
        INSERT INTO responsaveis_grupo (grupo_id, usuario_id)
        VALUES ($1::uuid, $2::uuid)
        ON CONFLICT DO NOTHING
      `, [group.id, req.user.id]);
    }

    res.status(201).json({
      message: 'Grupo criado com sucesso',
      group: {
        ...group,
        kind: linkedGroupIds.length ? 'aggregator' : 'zendesk',
        linked_group_ids: linkedGroupIds,
        consultants: [],
        manager_ids: req.user.role === 'manager' ? [req.user.id] : [],
      },
    });
  } catch(e) {
    if (e.code === '23505') {
      return res.status(409).json({ error: 'Ja existe um grupo com este nome ou id_zendesk' });
    }
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/groups/:groupId', authenticate, adminOrManagerOnly, async (req, res) => {
  const body = req.body || {};
  const name = String(body.name || '').replace(/\s+/g, ' ').trim();
  const linkedGroupIds = dedupeStringIds(body.linked_group_ids);

  if (!name) return res.status(400).json({ error: 'Informe o nome do grupo' });
  if (!linkedGroupIds.length) return res.status(400).json({ error: 'Selecione ao menos um grupo Zendesk para o agrupador' });

  try {
    await ensureOwnedManagedGroup(req.user, req.params.groupId);
    const [groupDef] = await fetchGroupDefinitions([req.params.groupId]);
    if (!groupDef) return res.status(404).json({ error: 'Grupo nao encontrado' });
    if (groupDef.kind !== 'aggregator') {
      return res.status(400).json({ error: 'Apenas agrupadores podem ser editados por esta tela' });
    }

    const childDefinitions = await fetchGroupDefinitions(linkedGroupIds);
    const foundIds = new Set(childDefinitions.map(def => String(def.id)));
    const invalidChild = linkedGroupIds.find(id => !foundIds.has(String(id)));
    if (invalidChild) return res.status(400).json({ error: 'Um dos grupos vinculados nao existe' });
    const invalidAggregator = childDefinitions.find(def => def.kind === 'aggregator');
    if (invalidAggregator) {
      return res.status(400).json({ error: 'Agrupadores nao podem ser vinculados dentro de outro agrupador' });
    }

    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await client.query(`
        UPDATE grupos
        SET nome = $1, atualizado_em = NOW()
        WHERE id = $2::uuid
      `, [name, req.params.groupId]);
      await client.query(`
        DELETE FROM grupo_agrupador_vinculos
        WHERE agrupador_id = $1::uuid
      `, [req.params.groupId]);
      for (const linkedGroupId of linkedGroupIds) {
        await client.query(`
          INSERT INTO grupo_agrupador_vinculos (agrupador_id, grupo_id)
          VALUES ($1::uuid, $2::uuid)
          ON CONFLICT DO NOTHING
        `, [req.params.groupId, linkedGroupId]);
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    const [updatedGroup] = await fetchGroupDefinitions([req.params.groupId]);
    res.json({
      message: 'Agrupador atualizado com sucesso',
      group: updatedGroup,
    });
  } catch(e) {
    if (e.code === '23505') {
      return res.status(409).json({ error: 'Ja existe um grupo com este nome' });
    }
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.post('/api/admin/groups/:groupId/manager', authenticate, adminOnly, async (req, res) => {
  const { manager_id, action } = req.body;
  try {
    if (!manager_id) {
      await db.query('DELETE FROM responsaveis_grupo WHERE grupo_id = $1', [req.params.groupId]);
    } else if (action === 'remove') {
      await db.query('DELETE FROM responsaveis_grupo WHERE grupo_id = $1 AND usuario_id = $2', [req.params.groupId, manager_id]);
    } else if (action === 'remove-all') {
      // Remove o usuário de todos os grupos de uma vez (chamado ao desmarcar Resp.)
      await db.query('DELETE FROM responsaveis_grupo WHERE usuario_id = $1', [manager_id]);
    } else {
      await db.query(`
        INSERT INTO responsaveis_grupo (grupo_id, usuario_id) VALUES ($1, $2) ON CONFLICT DO NOTHING
      `, [req.params.groupId, manager_id]);
      await db.query(`
        UPDATE usuarios SET papel = 'manager' WHERE id = $1 AND papel = 'consultant'
      `, [manager_id]);
    }
    res.json({ message: 'Responsáveis atualizados' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/groups/:groupId/member', authenticate, adminOrManagerOnly, async (req, res) => {
  const { user_id, action } = req.body || {};
  if (!user_id) return res.status(400).json({ error: 'user_id obrigatorio' });
  try {
    await ensureManageableGroup(req.user, req.params.groupId);
    const [groupDef] = await fetchGroupDefinitions([req.params.groupId]);
    if (!groupDef) return res.status(404).json({ error: 'Grupo nao encontrado' });
    const targetGroupIds = groupDef.kind === 'aggregator'
      ? dedupeStringIds(groupDef.linked_group_ids)
      : [String(groupDef.id)];
    if (!targetGroupIds.length) {
      return res.status(400).json({ error: 'O agrupador selecionado nao possui grupos Zendesk vinculados' });
    }

    const { rows: userRows } = await db.query(
      'SELECT id, papel AS role FROM usuarios WHERE id = $1::uuid LIMIT 1',
      [user_id]
    );
    if (!userRows.length) return res.status(404).json({ error: 'Usuario nao encontrado' });
    if (userRows[0].role === 'admin') return res.status(400).json({ error: 'Nao e permitido vincular administrador em grupos por esta tela' });

    if (action === 'remove') {
      await db.query(
        'DELETE FROM membros_grupo WHERE grupo_id = ANY($1::uuid[]) AND usuario_id = $2::uuid',
        [targetGroupIds, user_id]
      );
      return res.json({ message: 'Membro removido do grupo' });
    }

    for (const targetGroupId of targetGroupIds) {
      await db.query(`
        INSERT INTO membros_grupo (usuario_id, grupo_id) VALUES ($1::uuid, $2::uuid)
        ON CONFLICT DO NOTHING
      `, [user_id, targetGroupId]);
    }
    res.json({ message: 'Membro adicionado ao grupo' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   MANAGER: MEU GRUPO
══════════════════════════════════════════════════════════════ */

app.get('/api/manager/group', authenticate, async (req, res) => {
  try {
    const definitions = await fetchGroupDefinitions();
    const managedGroupIds = new Set(await getManagedGroupIdsByUserId(req.user.id));
    const myGroups = definitions
      .filter(def => managedGroupIds.has(String(def.id)))
      .map(def => ({ id: String(def.id), name: def.name, kind: def.kind }));
    if (!myGroups.length) return res.status(403).json({ error: 'Você não é responsável por nenhum grupo' });

    const grupoIds = await getManagedActualGroupIds(req.user.id);
    if (!grupoIds.length) {
      return res.json({ groups: myGroups, consultants: [], totalEvals: 0 });
    }

    const { rows: consultants } = await db.query(`
      SELECT DISTINCT u.id, u.nome AS name, u.email,
        u.id_zendesk AS zendesk_agent_id,
        COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids,
        COALESCE(array_agg(DISTINCT g2.nome)        FILTER (WHERE g2.nome IS NOT NULL),        '{}') AS groups
      FROM usuarios u
      JOIN membros_grupo mg ON mg.usuario_id = u.id AND mg.grupo_id = ANY($1)
      LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
      LEFT JOIN membros_grupo mg2 ON mg2.usuario_id = u.id AND mg2.grupo_id = ANY($1)
      LEFT JOIN grupos g2 ON g2.id = mg2.grupo_id
      WHERE u.id_zendesk IS NOT NULL
      GROUP BY u.id
      ORDER BY u.nome
    `, [grupoIds]);

    const allAssigneeIds = consultants.flatMap(c => c.assignee_ids);

    const { rows: evals } = await db.query(`
      SELECT * FROM avaliacoes WHERE id_assignee_zendesk = ANY($1)
    `, [allAssigneeIds]);

    const { rows: tickets } = await db.query(`
      SELECT id_assignee_zendesk, avaliado, descartado
      FROM tickets WHERE tipo = 'normal' AND id_assignee_zendesk = ANY($1)
    `, [allAssigneeIds]);

    const statsPerConsultant = consultants.map(c => {
      const myEvals = evals.filter(e => c.assignee_ids.includes(e.id_assignee_zendesk));
      const myTkts  = tickets.filter(t => c.assignee_ids.includes(t.id_assignee_zendesk));
      const avg = myEvals.length ? Math.round(myEvals.reduce((a,e) => a + e.nota_final, 0) / myEvals.length) : null;
      return {
        id: c.id, name: c.name, email: c.email, groups: c.groups,
        total:     myTkts.filter(t => !t.descartado).length,
        evaluated: myTkts.filter(t =>  t.avaliado).length,
        pending:   myTkts.filter(t => !t.avaliado && !t.descartado).length,
        avgScore: avg,
        lastEval: myEvals.sort((a,b) => new Date(b.criado_em)-new Date(a.criado_em))[0]?.criado_em || null
      };
    });

    res.json({ groups: myGroups, consultants: statsPerConsultant, totalEvals: evals.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   WEBHOOK
══════════════════════════════════════════════════════════════ */

app.get('/api/consultants/filters', authenticate, canManage, async (req, res) => {
  const { group_id, group_ids } = req.query;
  try {
    const scope = await loadConsultantsScopeForUser(req.user, group_ids || group_id);
    res.json({
      groups: scope.groups,
      consultants: scope.consultants.map(c => ({
        id: c.id,
        name: c.name,
        email: c.email || null,
        user_type: c.user_type || null,
        photo_url: c.photo_url || null,
        requester_id: c.requester_id ? String(c.requester_id) : null,
        assignee_ids: (c.assignee_ids || []).map(v => String(v)),
        group_ids: (c.group_ids || []).map(v => String(v)),
        group_names: c.group_names || [],
      })),
      selected_group_id: scope.selectedGroupIds[0] || null,
      selected_group_name: scope.selectedGroupNames[0] || null,
      selected_group_ids: scope.selectedGroupIds,
      selected_group_names: scope.selectedGroupNames,
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.get('/api/consultants/analytics', authenticate, canManage, async (req, res) => {
  const { group_id, group_ids, consultant_ids, month } = req.query;

  let period;
  try {
    period = parseMonthPeriod(month);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
  const csatPeriod = parseRollingDaysPeriod(60);

  try {
    const scope = await loadConsultantsScopeForUser(req.user, group_ids || group_id);
    const allConsultants = filterN1Consultants(scope.consultants);

    if (!scope.groups.length) {
      return res.json({
        month: period.month,
        period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
        csat_period: { start: csatPeriod.startUtc.toISOString(), end: csatPeriod.endUtc.toISOString(), days: csatPeriod.days },
        groups: [],
        consultants: [],
        selected_group_id: null,
        selected_group_name: null,
        selected_group_ids: [],
        selected_group_names: [],
        selected_consultant_ids: [],
        cards: buildEmptyConsultantsCards(),
      });
    }

    if (!scope.selectedGroupIds.length) {
      return res.status(400).json({ error: 'Selecione ao menos um grupo para gerar o ranking.' });
    }

    if (!allConsultants.length) {
      return res.json({
        month: period.month,
        period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
        csat_period: { start: csatPeriod.startUtc.toISOString(), end: csatPeriod.endUtc.toISOString(), days: csatPeriod.days },
        groups: scope.groups,
        consultants: [],
        selected_group_id: scope.selectedGroupIds[0] || null,
        selected_group_name: scope.selectedGroupNames[0] || null,
        selected_group_ids: scope.selectedGroupIds,
        selected_group_names: scope.selectedGroupNames,
        selected_consultant_ids: [],
        cards: buildEmptyConsultantsCards(),
      });
    }

    const consultantIdSet = new Set(allConsultants.map(c => String(c.id)));
    const explicitConsultantIds = parseCsvQueryIds(consultant_ids);
    const invalidConsultant = explicitConsultantIds.find(id => !consultantIdSet.has(String(id)));
    if (invalidConsultant) {
      return res.status(403).json({ error: 'Consultor invalido para os grupos selecionados' });
    }

    const effectiveConsultantIds = explicitConsultantIds.length
      ? explicitConsultantIds
      : allConsultants.map(c => String(c.id));
    const effectiveConsultantSet = new Set(effectiveConsultantIds);
    const consultants = allConsultants.filter(c => effectiveConsultantSet.has(String(c.id)));

    const assigneeToConsultant = {};
    consultants.forEach(c => {
      (c.assignee_ids || []).forEach(aid => {
        const key = String(aid);
        if (!assigneeToConsultant[key]) assigneeToConsultant[key] = c.id;
      });
    });

    const assigneeIds = [...new Set(
      consultants.flatMap(c => c.assignee_ids || []).map(v => String(v)).filter(Boolean)
    )];
    const allConsultantsAssigneeIds = [...new Set(
      allConsultants.flatMap(c => c.assignee_ids || []).map(v => String(v)).filter(Boolean)
    )];

    let atendimentoRows = [];
    let negativadosRows = [];
    let pdcaRows = [];

    if (assigneeIds.length) {
      atendimentoRows = (await db.query(`
        SELECT id_assignee_zendesk, COUNT(*)::int AS total, SUM(nota_final)::numeric AS sum_score
        FROM avaliacoes
        WHERE origem = 'normal'
          AND criado_em >= $1
          AND criado_em <  $2
          AND id_assignee_zendesk = ANY($3)
        GROUP BY id_assignee_zendesk
      `, [period.startUtc.toISOString(), period.endUtc.toISOString(), assigneeIds])).rows;

      negativadosRows = (await db.query(`
        SELECT id_assignee_zendesk, COUNT(*)::int AS total, SUM(nota_final)::numeric AS sum_score
        FROM avaliacoes
        WHERE origem = 'csat'
          AND COALESCE(nao_totalizar_negativado, false) = false
          AND criado_em >= $1
          AND criado_em <  $2
          AND id_assignee_zendesk = ANY($3)
        GROUP BY id_assignee_zendesk
      `, [period.startUtc.toISOString(), period.endUtc.toISOString(), assigneeIds])).rows;

    }

    const atendimento = buildScoreRanking(consultants, assigneeToConsultant, atendimentoRows);
    const negativados = buildNegativadosRanking(consultants, assigneeToConsultant, negativadosRows);

    // PDCA: usa a nota recebida no ticket PDCA (extraída do texto "Nota: X"),
    // agrupando por consultor no mês selecionado em média simples (soma / quantidade).
    const requesterToConsultant = {};
    const requesterIds = [];
    consultants.forEach(c => {
      const requesterId = String(c.requester_id || '').trim();
      if (!requesterId) return;
      requesterIds.push(requesterId);
      if (!requesterToConsultant[requesterId]) requesterToConsultant[requesterId] = c.id;
    });

    const pdcaTicketRows = (await db.query(`
      SELECT
        consultor_id::text AS consultor_id,
        id_assignee_zendesk,
        descricao
      FROM pdca_tickets
      WHERE COALESCE(resolvido_no_zendesk, criado_no_zendesk, importado_em) >= $1
        AND COALESCE(resolvido_no_zendesk, criado_no_zendesk, importado_em) <  $2
        AND (
          consultor_id = ANY($3::uuid[])
          OR id_assignee_zendesk = ANY($4::text[])
        )
    `, [
      period.startUtc.toISOString(),
      period.endUtc.toISOString(),
      effectiveConsultantIds,
      requesterIds,
    ])).rows;

    const pdcaAcc = {};
    pdcaTicketRows.forEach(row => {
      const directConsultantId = String(row.consultor_id || '').trim();
      const mappedConsultantId = String(requesterToConsultant[String(row.id_assignee_zendesk || '').trim()] || '').trim();
      const cid = effectiveConsultantSet.has(directConsultantId) ? directConsultantId : mappedConsultantId;
      if (!cid || !effectiveConsultantSet.has(cid)) return;

      const score = extractPdcaScore(row.descricao);
      if (score === null || score === undefined || Number.isNaN(Number(score))) return;

      if (!pdcaAcc[cid]) pdcaAcc[cid] = { sum: 0, total: 0 };
      pdcaAcc[cid].sum += Number(score);
      pdcaAcc[cid].total += 1;
    });

    pdcaRows = Object.entries(pdcaAcc).map(([cid, v]) => ({
      id_assignee_zendesk: String(cid),
      total: Number(v.total || 0),
      sum_score: Number(v.sum || 0),
    }));

    const pdcaKeyMap = {};
    consultants.forEach(c => { pdcaKeyMap[String(c.id)] = c.id; });
    const pdca = buildScoreRanking(consultants, pdcaKeyMap, pdcaRows);

    const cfg = await getCfgForUser(req.user);
    let csatSource = 'zendesk';
    let csatError = null;
    let csatRatings = [];
    let csatCountRows = null;
    let totalAtendimentosSource = 'zendesk';
    let totalAtendimentosError = null;
    let totalAtendimentos = buildCountRanking(consultants, {});

    if (!cfg.zendesk_subdominio || !cfg.zendesk_email || !cfg.zendesk_token) {
      csatSource = 'not_configured';
      totalAtendimentosSource = 'not_configured';
    } else {
      if (assigneeIds.length) {
        try {
          const allPeriodRatings = await fetchZendeskCsatByPeriodPerAssignee(cfg, csatPeriod.startUtc, csatPeriod.endUtc, assigneeIds);
          csatRatings = allPeriodRatings.filter(r => assigneeToConsultant[String(r.assignee_id || '')]);
          csatSource = 'zendesk_assignee';
        } catch (e) {
          const msg = String(e?.message || '');
          if (msg.includes('HTTP 400') || String(e?.code || '') === 'ASSIGNEE_QUERY_FAILED') {
            try {
              csatCountRows = await fetchZendeskCsatCountsBySearch(
                cfg,
                csatPeriod.startUtc,
                csatPeriod.endUtc,
                new Set(assigneeIds.map(v => String(v)))
              );
              csatSource = 'zendesk_search';
              csatError = null;
            } catch (fallbackErr) {
              csatSource = 'error';
              csatError = `${msg} | fallback search: ${String(fallbackErr?.message || 'erro desconhecido')}`;
            }
          } else {
            csatSource = 'error';
            csatError = msg;
          }
        }
      }

      if (allConsultantsAssigneeIds.length) {
        try {
          const assigneeTotals = {};
          for (const aid of allConsultantsAssigneeIds) {
            assigneeTotals[String(aid)] = await fetchZendeskTicketsCountByAssigneeAndPeriod(cfg, aid, period.startUtc, period.endUtc);
          }
          const consultantTotals = {};
          allConsultants.forEach(c => {
            consultantTotals[c.id] = (c.assignee_ids || [])
              .map(aid => Number(assigneeTotals[String(aid)] || 0))
              .reduce((s, v) => s + v, 0);
          });
          totalAtendimentos = buildCountRanking(consultants, consultantTotals, allConsultants);
        } catch (e) {
          totalAtendimentosSource = 'error';
          totalAtendimentosError = e.message;
        }
      }
    }

    const csat = Array.isArray(csatCountRows)
      ? buildCsatRankingFromCountRows(consultants, assigneeToConsultant, csatCountRows)
      : buildCsatRanking(consultants, assigneeToConsultant, csatRatings);
    let peerReviewsSource = 'google_sheets';
    let peerReviewsError = null;
    let peerReviewsCard = {
      ...buildPeerReviewsRanking(consultants, period, []),
      source: peerReviewsSource,
      error: peerReviewsError,
      fallback_month: null,
      used_fallback: false,
    };
    try {
      const peerRows = await fetchPeerReviewsRowsFromGoogle();
      peerReviewsCard = {
        ...buildPeerReviewsRanking(consultants, period, peerRows),
        source: peerReviewsSource,
        error: peerReviewsError,
        fallback_month: null,
        used_fallback: false,
      };
      const previousPeerRows = await fetchPreviousMonthPeerReviewHistory(period.month, consultants);
      peerReviewsCard = applyPeerReviewsPreviousMonthFallback(
        peerReviewsCard,
        previousPeerRows,
        consultants,
        getPreviousMonthRef(period.month)
      );
      peerReviewsSource = peerReviewsCard.source || peerReviewsSource;
    } catch (e) {
      if (e.code === 'GOOGLE_SHEETS_NOT_CONFIGURED') {
        peerReviewsSource = 'not_configured';
      } else {
        peerReviewsSource = 'error';
        peerReviewsError = e.message;
      }
      peerReviewsCard = {
        ...peerReviewsCard,
        source: peerReviewsSource,
        error: peerReviewsError,
      };
    }

    const cards = {
      csat: { ...csat, source: csatSource, error: csatError },
      atendimento,
      negativados,
      pdca,
      total_atendimentos: { ...totalAtendimentos, source: totalAtendimentosSource, error: totalAtendimentosError },
      avaliacoes_pares: { ...peerReviewsCard, source: peerReviewsSource, error: peerReviewsError },
    };
    cards.indice_tecnico = buildTechnicalIndexCard(cards, consultants);

    const savedRows = await saveConsultantMonthlyHistory(period.month, consultants, cards);

    res.json({
      month: period.month,
      period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
      csat_period: { start: csatPeriod.startUtc.toISOString(), end: csatPeriod.endUtc.toISOString(), days: csatPeriod.days },
      groups: scope.groups,
      consultants: allConsultants.map(c => ({
        id: c.id,
        name: c.name,
        email: c.email || null,
        user_type: c.user_type || null,
        photo_url: c.photo_url || null,
        requester_id: c.requester_id ? String(c.requester_id) : null,
        assignee_ids: (c.assignee_ids || []).map(v => String(v)),
        group_ids: (c.group_ids || []).map(v => String(v)),
        group_names: c.group_names || [],
      })),
      selected_group_id: scope.selectedGroupIds[0] || null,
      selected_group_name: scope.selectedGroupNames[0] || null,
      selected_group_ids: scope.selectedGroupIds,
      selected_group_names: scope.selectedGroupNames,
      selected_consultant_ids: explicitConsultantIds.map(v => String(v)),
      history_saved: savedRows,
      cards,
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.get('/api/consultants/analytics/last', authenticate, canManage, async (req, res) => {
  const { group_id, group_ids, consultant_ids } = req.query;
  const requestedGroupIds = parseCsvQueryIds(group_ids || group_id);
  try {
    let scope;
    if (requestedGroupIds.length) {
      scope = await loadConsultantsScopeForUser(req.user, requestedGroupIds.join(','));
    } else {
      const baseScope = await loadConsultantsScopeForUser(req.user, null);
      const allGroupIds = (baseScope.groups || []).map(g => String(g.id));
      scope = allGroupIds.length
        ? await loadConsultantsScopeForUser(req.user, allGroupIds.join(','))
        : {
            groups: baseScope.groups || [],
            consultants: [],
            selectedGroupIds: [],
            selectedGroupNames: [],
          };
    }

    const allConsultants = filterN1Consultants(scope.consultants || []);
    const explicitConsultantIds = parseCsvQueryIds(consultant_ids);
    const consultantIdSet = new Set(allConsultants.map(c => String(c.id)));
    const invalidConsultant = explicitConsultantIds.find(id => !consultantIdSet.has(String(id)));
    if (invalidConsultant) {
      return res.status(403).json({ error: 'Consultor invalido para os grupos selecionados' });
    }

    const effectiveConsultantIds = explicitConsultantIds.length
      ? explicitConsultantIds
      : allConsultants.map(c => String(c.id));
    const effectiveConsultantSet = new Set(effectiveConsultantIds);
    const selectedConsultants = allConsultants.filter(c => effectiveConsultantSet.has(String(c.id)));

    if (!scope.groups.length || !selectedConsultants.length) {
      return res.json({
        has_history: false,
        month: null,
        period: null,
        groups: scope.groups || [],
        consultants: allConsultants.map(c => ({
          id: c.id,
          name: c.name,
          email: c.email || null,
          user_type: c.user_type || null,
          photo_url: c.photo_url || null,
          requester_id: c.requester_id ? String(c.requester_id) : null,
          assignee_ids: (c.assignee_ids || []).map(v => String(v)),
          group_ids: (c.group_ids || []).map(v => String(v)),
          group_names: c.group_names || [],
        })),
        selected_group_id: scope.selectedGroupIds?.[0] || null,
        selected_group_name: scope.selectedGroupNames?.[0] || null,
        selected_group_ids: scope.selectedGroupIds || [],
        selected_group_names: scope.selectedGroupNames || [],
        selected_consultant_ids: explicitConsultantIds.map(v => String(v)),
        cards: buildEmptyConsultantsCards(),
      });
    }

    const { rows: latestRows } = await db.query(`
      SELECT MAX(mes_ref)::date AS mes_ref
      FROM historico_csat_consultor
      WHERE consultor_id = ANY($1::uuid[])
    `, [effectiveConsultantIds]);

    const latestMonthDate = latestRows[0]?.mes_ref || null;
    if (!latestMonthDate) {
      return res.json({
        has_history: false,
        month: null,
        period: null,
        groups: scope.groups || [],
        consultants: allConsultants.map(c => ({
          id: c.id,
          name: c.name,
          email: c.email || null,
          user_type: c.user_type || null,
          photo_url: c.photo_url || null,
          requester_id: c.requester_id ? String(c.requester_id) : null,
          assignee_ids: (c.assignee_ids || []).map(v => String(v)),
          group_ids: (c.group_ids || []).map(v => String(v)),
          group_names: c.group_names || [],
        })),
        selected_group_id: scope.selectedGroupIds?.[0] || null,
        selected_group_name: scope.selectedGroupNames?.[0] || null,
        selected_group_ids: scope.selectedGroupIds || [],
        selected_group_names: scope.selectedGroupNames || [],
        selected_consultant_ids: explicitConsultantIds.map(v => String(v)),
        cards: buildEmptyConsultantsCards(),
      });
    }

    const latestMonth = typeof latestMonthDate === 'string'
      ? latestMonthDate.slice(0, 7)
      : `${latestMonthDate.getUTCFullYear()}-${String(latestMonthDate.getUTCMonth() + 1).padStart(2, '0')}`;
    const period = parseMonthPeriod(latestMonth);

    const { rows: historyRows } = await db.query(`
      SELECT
        h.consultor_id::text AS consultant_id,
        u.nome AS consultant_name,
        h.csat_percent,
        h.total_avaliacoes,
        h.positivos,
        h.negativos,
        h.fonte,
        h.erro,
        h.atendimento_percent,
        h.atendimento_total,
        h.negativados_percent,
        h.negativados_total,
        h.pdca_percent,
        h.pdca_total,
        h.total_atendimentos,
        h.fonte_total_atendimentos,
        h.erro_total_atendimentos,
        h.avaliacoes_pares_percent,
        h.avaliacoes_pares_total,
        h.avaliacoes_pares_par_percent,
        h.avaliacoes_pares_par_total,
        h.avaliacoes_pares_gestor_percent,
        h.avaliacoes_pares_gestor_total,
        h.fonte_avaliacoes_pares,
        h.erro_avaliacoes_pares
      FROM historico_csat_consultor h
      LEFT JOIN usuarios u ON u.id = h.consultor_id
      WHERE h.mes_ref = $1::date
        AND h.consultor_id = ANY($2::uuid[])
    `, [`${latestMonth}-01`, effectiveConsultantIds]);

    const cards = buildConsultantsCardsFromCsatHistoryRows(selectedConsultants, historyRows, allConsultants);

    res.json({
      has_history: true,
      month: latestMonth,
      period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
      groups: scope.groups || [],
      consultants: allConsultants.map(c => ({
        id: c.id,
        name: c.name,
        email: c.email || null,
        user_type: c.user_type || null,
        photo_url: c.photo_url || null,
        requester_id: c.requester_id ? String(c.requester_id) : null,
        assignee_ids: (c.assignee_ids || []).map(v => String(v)),
        group_ids: (c.group_ids || []).map(v => String(v)),
        group_names: c.group_names || [],
      })),
      selected_group_id: scope.selectedGroupIds?.[0] || null,
      selected_group_name: scope.selectedGroupNames?.[0] || null,
      selected_group_ids: scope.selectedGroupIds || [],
      selected_group_names: scope.selectedGroupNames || [],
      selected_consultant_ids: explicitConsultantIds.map(v => String(v)),
      cards,
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.get('/api/consultant/technical-index', authenticate, async (req, res) => {
  if (req.user.role !== 'consultant') {
    return res.status(403).json({ error: 'Acesso restrito ao consultor logado' });
  }

  const requestedMonth = String(req.query.month || '').trim();
  let period = null;
  if (requestedMonth) {
    try {
      period = parseMonthPeriod(requestedMonth);
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }
  }

  try {
    const baseScope = await loadConsultantsScopeForUser(req.user, null);
    const allGroupIds = (baseScope.groups || []).map(g => String(g.id));
    if (!allGroupIds.length) {
      return res.json({
        has_history: false,
        month: requestedMonth || null,
        period: period ? { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() } : null,
        groups: [],
        me: { id: req.user.id, name: req.user.name || null, email: req.user.email || null },
        consultants_count: 0,
        metrics: {
          indice_tecnico: buildMyMetricView(buildEmptyConsultantsCards().indice_tecnico, req.user.id),
          csat: buildMyMetricView(buildEmptyConsultantsCards().csat, req.user.id),
          atendimento: buildMyMetricView(buildEmptyConsultantsCards().atendimento, req.user.id),
          negativados: buildMyMetricView(buildEmptyConsultantsCards().negativados, req.user.id),
          pdca: buildMyMetricView(buildEmptyConsultantsCards().pdca, req.user.id),
          total_atendimentos: buildMyMetricView(buildEmptyConsultantsCards().total_atendimentos, req.user.id),
          avaliacoes_pares: buildMyMetricView(buildEmptyConsultantsCards().avaliacoes_pares, req.user.id),
        },
      });
    }

    const scope = await loadConsultantsScopeForUser(req.user, allGroupIds.join(','));
    const consultants = scope.consultants || [];
    const me = consultants.find(c => String(c.id) === String(req.user.id));
    if (!me) {
      return res.status(404).json({ error: 'Consultor logado nao encontrado nos grupos vinculados' });
    }

    const consultantIds = consultants.map(c => String(c.id));
    let monthRef = period ? period.month : '';
    if (!monthRef) {
      const { rows: latestRows } = await db.query(`
        SELECT MAX(mes_ref)::date AS mes_ref
        FROM historico_csat_consultor
        WHERE consultor_id = ANY($1::uuid[])
      `, [consultantIds]);
      const latestMonthDate = latestRows[0]?.mes_ref || null;
      if (!latestMonthDate) {
        const emptyCards = buildEmptyConsultantsCards();
        return res.json({
          has_history: false,
          month: null,
          period: null,
          groups: scope.groups || [],
          me: { id: me.id, name: me.name, email: me.email || null },
          consultants_count: consultants.length,
          metrics: {
            indice_tecnico: buildMyMetricView(emptyCards.indice_tecnico, me.id),
            csat: buildMyMetricView(emptyCards.csat, me.id),
            atendimento: buildMyMetricView(emptyCards.atendimento, me.id),
            negativados: buildMyMetricView(emptyCards.negativados, me.id),
            pdca: buildMyMetricView(emptyCards.pdca, me.id),
            total_atendimentos: buildMyMetricView(emptyCards.total_atendimentos, me.id),
            avaliacoes_pares: buildMyMetricView(emptyCards.avaliacoes_pares, me.id),
          },
        });
      }
      monthRef = typeof latestMonthDate === 'string'
        ? latestMonthDate.slice(0, 7)
        : `${latestMonthDate.getUTCFullYear()}-${String(latestMonthDate.getUTCMonth() + 1).padStart(2, '0')}`;
      period = parseMonthPeriod(monthRef);
    }

    const { rows: historyRows } = await db.query(`
      SELECT
        h.consultor_id::text AS consultant_id,
        u.nome AS consultant_name,
        h.csat_percent,
        h.total_avaliacoes,
        h.positivos,
        h.negativos,
        h.fonte,
        h.erro,
        h.atendimento_percent,
        h.atendimento_total,
        h.negativados_percent,
        h.negativados_total,
        h.pdca_percent,
        h.pdca_total,
        h.total_atendimentos,
        h.fonte_total_atendimentos,
        h.erro_total_atendimentos,
        h.avaliacoes_pares_percent,
        h.avaliacoes_pares_total,
        h.avaliacoes_pares_par_percent,
        h.avaliacoes_pares_par_total,
        h.avaliacoes_pares_gestor_percent,
        h.avaliacoes_pares_gestor_total,
        h.fonte_avaliacoes_pares,
        h.erro_avaliacoes_pares
      FROM historico_csat_consultor h
      LEFT JOIN usuarios u ON u.id = h.consultor_id
      WHERE h.mes_ref = $1::date
        AND h.consultor_id = ANY($2::uuid[])
    `, [`${monthRef}-01`, consultantIds]);

    if (!historyRows.length) {
      const emptyCards = buildEmptyConsultantsCards();
      return res.json({
        has_history: false,
        month: monthRef,
        period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
        groups: scope.groups || [],
        me: { id: me.id, name: me.name, email: me.email || null },
        consultants_count: consultants.length,
        metrics: {
          indice_tecnico: buildMyMetricView(emptyCards.indice_tecnico, me.id),
          csat: buildMyMetricView(emptyCards.csat, me.id),
          atendimento: buildMyMetricView(emptyCards.atendimento, me.id),
          negativados: buildMyMetricView(emptyCards.negativados, me.id),
          pdca: buildMyMetricView(emptyCards.pdca, me.id),
          total_atendimentos: buildMyMetricView(emptyCards.total_atendimentos, me.id),
          avaliacoes_pares: buildMyMetricView(emptyCards.avaliacoes_pares, me.id),
        },
      });
    }

    const cards = buildConsultantsCardsFromCsatHistoryRows(consultants, historyRows, consultants);

    res.json({
      has_history: true,
      month: monthRef,
      period: { start: period.startUtc.toISOString(), end: period.endUtc.toISOString() },
      groups: scope.groups || [],
      me: { id: me.id, name: me.name, email: me.email || null },
      consultants_count: consultants.length,
      metrics: {
        indice_tecnico: buildMyMetricView(cards.indice_tecnico, me.id),
        csat: buildMyMetricView(cards.csat, me.id),
        atendimento: buildMyMetricView(cards.atendimento, me.id),
        negativados: buildMyMetricView(cards.negativados, me.id),
        pdca: buildMyMetricView(cards.pdca, me.id),
        total_atendimentos: buildMyMetricView(cards.total_atendimentos, me.id),
        avaliacoes_pares: buildMyMetricView(cards.avaliacoes_pares, me.id),
      },
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({ error: e.message });
  }
});

app.post('/webhook/zendesk', async (req, res) => {
  try {
    const p = req.body, t = p.ticket || p;
    const aid = String(t.assignee_id || t.assignee?.id || p.assignee_id || '');
    const zid = String(t.id || p.id || uuidv4());
    const { rows } = await db.query('SELECT id FROM tickets WHERE id_zendesk = $1', [zid]);
    if (rows.length) return res.status(200).json({ message: 'Ja registrado' });
    const row = await buildTicketRow(zid, aid, t, p, 'normal');
    await db.query(`
      INSERT INTO tickets (id_zendesk,tipo,assunto,descricao,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,nome_cliente,email_cliente,tags,iniciado_por_ia,criado_no_zendesk,resolvido_no_zendesk)
      VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9,$10,$11::varchar,$12,$13,$14::timestamptz,$15::timestamptz)
    `, [row.id_zendesk,row.tipo,row.assunto,row.descricao,row.status,row.canal,row.consultor_id,row.id_assignee_zendesk,row.nome_consultor,row.nome_cliente,row.email_cliente,row.tags,row.iniciado_por_ia,row.criado_no_zendesk,row.resolvido_no_zendesk]);
    res.status(200).json({ message: 'Ticket recebido', ticket_id: zid });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   IMPORTAR DO ZENDESK
══════════════════════════════════════════════════════════════ */

app.get('/api/zendesk/import', authenticate, canEvaluate, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { assignee_id, assignee_ids, group_ids, year, month, ticket_ids } = req.query;

  try {
    const amap = await getAssigneeMap();
    const { rows: existingRows } = await db.query('SELECT id_zendesk FROM tickets');
    const existing = new Set(existingRows.map(r => r.id_zendesk));
    
    // Buscar tickets descartados para não mostrar novamente
    const { rows: descartadosRows } = await db.query('SELECT DISTINCT id_zendesk FROM tickets_descartados');
    const descartados = new Set(descartadosRows.map(r => r.id_zendesk));

    const selectedGroupIds = parseCsvQueryIds(group_ids);
    const assigneeList = assignee_ids ? parseCsvQueryIds(assignee_ids) : parseCsvQueryIds(assignee_id);
    let zendeskGroupIds = [];
    let zendeskGroupIdsByAssignee = new Map();
    if (selectedGroupIds.length) {
      const actualGroupIds = await resolveEffectiveGroupIdsToActualGroupIds(selectedGroupIds);
      if (!actualGroupIds.length) {
        return res.status(400).json({ error: 'Nenhum grupo Zendesk vinculado foi encontrado para o agrupador selecionado' });
      }
      let importGroupIds = actualGroupIds;
      let importGroupIdsByAssignee = {};
      if (assigneeList.length) {
        const groupsByAssignee = await fetchActualGroupIdsByAssigneeIds(assigneeList);
        const actualGroupSet = new Set(actualGroupIds);
        importGroupIdsByAssignee = Object.fromEntries(assigneeList.map((aid) => [
          String(aid),
          (groupsByAssignee[String(aid)] || []).filter(groupId => actualGroupSet.has(String(groupId))),
        ]));
        importGroupIds = dedupeStringIds(Object.values(importGroupIdsByAssignee).flat());
        if (!importGroupIds.length) {
          return res.status(400).json({ error: 'Nenhum subgrupo vinculado foi encontrado para o consultor selecionado no agrupador selecionado' });
        }
      }
      const definitions = await fetchGroupDefinitions(importGroupIds);
      const zendeskGroupEntries = definitions
        .filter(def => def.kind !== 'aggregator' && String(def.id_zendesk || '').trim())
        .map(def => [String(def.id), String(def.id_zendesk || '').trim()]);
      const zendeskGroupIdByActualGroupId = new Map(zendeskGroupEntries);
      zendeskGroupIds = zendeskGroupEntries.map(([, zendeskId]) => zendeskId);
      if (Object.keys(importGroupIdsByAssignee).length) {
        zendeskGroupIdsByAssignee = new Map(Object.entries(importGroupIdsByAssignee).map(([aid, groupIds]) => [
          String(aid),
          new Set(groupIds.map(groupId => zendeskGroupIdByActualGroupId.get(String(groupId))).filter(Boolean)),
        ]));
      }
      if (!zendeskGroupIds.length) {
        return res.status(400).json({ error: 'Nenhum grupo selecionado possui id_zendesk valido' });
      }
    }
    const zendeskGroupSet = new Set(zendeskGroupIds);
    const ticketInSelectedGroup = (t, aid = null) => {
      const assigneeKey = String(aid || '');
      if (assigneeKey && zendeskGroupIdsByAssignee.has(assigneeKey)) {
        return zendeskGroupIdsByAssignee.get(assigneeKey).has(String(t?.group_id || ''));
      }
      if (!zendeskGroupSet.size) return true;
      return zendeskGroupSet.has(String(t?.group_id || ''));
    };
    const ticketFieldMap = await fetchZendeskTicketFieldMap(cfg).catch(() => ({}));

    const mapTicket = (t, aid) => {
      const meta = extractImportTicketMeta(t, ticketFieldMap);
      return {
        id: String(t.id), zendesk_id: String(t.id),
        subject: t.subject || 'Sem assunto',
        requester_name: t.via?.source?.from?.name || t.requester?.name || 'Cliente',
        assignee_id: String(aid || t.assignee_id || ''),
        consultant_name: amap[String(aid || t.assignee_id)] || 'ID:'+(aid || t.assignee_id),
        ticket_date: t.created_at || t.updated_at || t.solved_at || null,
        solved_at: t.created_at || t.updated_at || t.solved_at || null,
        categoria: meta.categoria,
        documento: meta.documento,
        produto: meta.produto,
        tags: t.tags || [],
        ai_initiated: (t.tags || []).includes('claudia_escalado_n2'),
        already_imported: existing.has(String(t.id)),
        already_discarded: descartados.has(String(t.id))
      };
    };

    // Modo 1: busca por números de ticket específicos
    if (ticket_ids) {
      const ids = String(ticket_ids).split(',').map(s => s.trim()).filter(Boolean);
      if (!ids.length) return res.status(400).json({ error: 'Nenhum ID informado' });
      const data = await zendeskRequest(`/api/v2/tickets/show_many.json?ids=${ids.join(',')}`, cfg);
      const tickets = (data.tickets || [])
        .filter(t => ticketInSelectedGroup(t))
        .map(t => mapTicket(t, null))
        .filter(t => !t.already_discarded);
      return res.json({ tickets, total: tickets.length });
    }

    // Modo 2: busca por consultor(es) + mês
    if (!assigneeList.length || !year || !month) return res.status(400).json({ error: 'assignee_id(s), year e month são obrigatórios' });
    
    const y = Number(year), m = Number(month);
    const start = new Date(y, m-1, 1).toISOString().slice(0,10);
    const end   = new Date(y, m, 0).toISOString().slice(0,10);
    
    // Busca tickets para cada assignee_id
    let allTickets = [];
    for (const aid of assigneeList) {
      try {
        const q = encodeURIComponent(`type:ticket assignee_id:${aid} created>=${start} created<=${end}`);
        const results = await fetchZendeskSearchResults(
          '/api/v2/search.json?query='+q+'&per_page=100&sort_by=created_at&sort_order=desc',
          cfg,
          'IMPORT_ATENDIMENTOS'
        );
        const tickets = results
          .filter(t => ticketInSelectedGroup(t, aid))
          .map(t => mapTicket(t, aid));
        allTickets = allTickets.concat(tickets);
      } catch (e) {
        console.warn('[IMPORT] Erro ao buscar tickets para assignee', aid, ':', e.message);
      }
    }
    
    // Remove duplicatas (caso um ticket tenha sido reatribuído) e descartados
    const seen = new Map();
    allTickets.forEach(t => {
      if (!seen.has(t.zendesk_id) && !t.already_discarded) {
        seen.set(t.zendesk_id, t);
      }
    });
    
    const uniqueTickets = Array.from(seen.values());
    res.json({ tickets: uniqueTickets, total: uniqueTickets.length });
  } catch (e) { res.status(500).json({ error: 'Erro Zendesk: '+e.message }); }
});

app.post('/api/zendesk/import', authenticate, canEvaluate, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { ticket_ids } = req.body;
  if (!ticket_ids?.length) return res.status(400).json({ error: 'Nenhum ticket selecionado' });
  const { rows: existingRows } = await db.query('SELECT id_zendesk FROM tickets');
  const existing = new Set(existingRows.map(r => r.id_zendesk));
  const amap = await getAssigneeMap();
  let imported = 0;
  for (const zid of ticket_ids) {
    if (existing.has(String(zid))) continue;
    try {
      const d = await zendeskRequest('/api/v2/tickets/'+zid+'.json', cfg);
      const t = d.ticket;
      const row = await buildTicketRow(String(t.id), String(t.assignee_id||''), t, t, 'normal', amap);
      await db.query(`
        INSERT INTO tickets (id_zendesk,tipo,assunto,descricao,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,nome_cliente,email_cliente,tags,iniciado_por_ia,criado_no_zendesk,resolvido_no_zendesk)
        VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9,$10,$11::varchar,$12,$13,$14::timestamptz,$15::timestamptz)
        ON CONFLICT (id_zendesk) DO NOTHING
      `, [row.id_zendesk,row.tipo,row.assunto,row.descricao,row.status,row.canal,row.consultor_id,row.id_assignee_zendesk,row.nome_consultor,row.nome_cliente,row.email_cliente,row.tags,row.iniciado_por_ia,row.criado_no_zendesk,row.resolvido_no_zendesk]);
      imported++;
    } catch (e) { console.warn('[IMPORT]', zid, e.message); }
  }
  res.json({ message: imported+' ticket(s) importado(s)', imported });
});

/* ══════════════════════════════════════════════════════════════
   TICKETS
══════════════════════════════════════════════════════════════ */

app.get('/api/pdca/import', authenticate, canManage, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { requester_id, requester_ids, year, month } = req.query;

  const requesterList = [...new Set(
    (requester_ids
      ? String(requester_ids).split(',').map(s => s.trim()).filter(Boolean)
      : (requester_id ? [String(requester_id).trim()] : [])
    ).map(String)
  )];
  if (!requesterList.length || !year || !month) {
    return res.status(400).json({ error: 'requester_id(s), year e month sao obrigatorios' });
  }

  const y = Number(year), m = Number(month);
  if (!y || !m || m < 1 || m > 12) return res.status(400).json({ error: 'Ano/mes invalido' });
  const start = new Date(y, m - 1, 1).toISOString().slice(0, 10);
  const end = new Date(y, m, 0).toISOString().slice(0, 10);

  try {
    if (req.user.role === 'manager') {
      const managerRequesterIds = await getManagerRequesterIds(req.user.id);
      const allowed = new Set((managerRequesterIds || []).map(String));
      const invalid = requesterList.find(id => !allowed.has(String(id)));
      if (invalid) return res.status(403).json({ error: 'Consultor/requester fora do escopo do responsavel' });
    }

    const requesterMap = await getRequesterConsultantMap();
    const { rows: existingRows } = await db.query('SELECT id_zendesk FROM pdca_tickets');
    const existing = new Set(existingRows.map(r => String(r.id_zendesk)));

    const mapTicket = (t, requesterId) => ({
      id: String(t.id),
      zendesk_id: String(t.id),
      subject: t.subject || 'Sem assunto',
      requester_name: t.via?.source?.from?.name || t.requester?.name || 'Cliente',
      requester_id: String(requesterId || t.requester_id || ''),
      consultant_name: requesterMap[String(requesterId || t.requester_id || '')]?.name || ('ID:' + (requesterId || t.requester_id || '')),
      solved_at: t.updated_at || t.solved_at || null,
      tags: t.tags || [],
      already_imported: existing.has(String(t.id)),
      description: t.description || ''
    });

    let allTickets = [];
    for (const requesterId of requesterList) {
      try {
        const q = encodeURIComponent(`type:ticket status<solved tags:motivo_pdca created>=${start} created<=${end} requester_id:${requesterId}`);
        const data = await zendeskRequest('/api/v2/search.json?query=' + q + '&per_page=100&sort_by=solved_at&sort_order=desc', cfg, 'IMPORT_PDCA');
        const tickets = (data.results || [])
          .filter(t => Array.isArray(t.tags) && t.tags.some(tag => String(tag).toLowerCase() === 'motivo_pdca'))
          .map(t => ({
            ...mapTicket(t, requesterId),
            requester_id: String(requesterId),
          }));
        allTickets = allTickets.concat(tickets);
      } catch (e) {
        console.warn('[PDCA IMPORT] Erro ao buscar tickets para requester', requesterId, ':', e.message);
      }
    }

    const seen = new Map();
    allTickets.forEach(t => {
      if (!seen.has(t.zendesk_id)) seen.set(t.zendesk_id, t);
    });
    const uniqueTickets = Array.from(seen.values());
    res.json({ tickets: uniqueTickets, total: uniqueTickets.length });
  } catch (e) {
    res.status(500).json({ error: 'Erro Zendesk: ' + e.message });
  }
});

app.post('/api/pdca/import', authenticate, canManage, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { ticket_ids } = req.body;
  if (!ticket_ids?.length) return res.status(400).json({ error: 'Nenhum ticket selecionado' });

  try {
    const { rows: existingRows } = await db.query('SELECT id_zendesk FROM pdca_tickets');
    const existing = new Set(existingRows.map(r => String(r.id_zendesk)));
    const requesterMap = await getRequesterConsultantMap();
    const amap = await getAssigneeMap();
    let imported = 0;

    const managerAllowed = req.user.role === 'manager'
      ? new Set(((await getManagerRequesterIds(req.user.id)) || []).map(String))
      : null;

    for (const zid of ticket_ids.map(String)) {
      if (existing.has(zid)) continue;
      try {
        const d = await zendeskRequest('/api/v2/tickets/' + zid + '.json', cfg);
        const t = d.ticket;
        const requesterId = String(t.requester_id || '');
        if (!requesterId) continue;
        if (managerAllowed && !managerAllowed.has(requesterId)) continue;

        const hasPdcaTag = Array.isArray(t.tags) && t.tags.some(tag => String(tag).toLowerCase() === 'motivo_pdca');
        if (!hasPdcaTag) continue;

        const row = await buildTicketRow(String(t.id), String(t.assignee_id || ''), t, t, 'normal', amap);
        let interactionsText = '';
        try {
          interactionsText = await fetchZendeskTicketInteractions(cfg, t.id);
        } catch (errComments) {
          console.warn('[PDCA IMPORT] Falha ao buscar interacoes do ticket', zid, ':', errComments.message);
        }
        const mergedDescription = mergeDescriptions(row.descricao, interactionsText);
        const requesterConsultant = requesterMap[requesterId] || null;
        await db.query(`
          INSERT INTO pdca_tickets (id_zendesk,assunto,descricao,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,nome_cliente,email_cliente,tags,iniciado_por_ia,criado_no_zendesk,resolvido_no_zendesk)
          VALUES ($1,$2,$3,$4,$5,$6::uuid,$7,$8,$9,$10::varchar,$11,$12,$13::timestamptz,$14::timestamptz)
          ON CONFLICT (id_zendesk) DO NOTHING
        `, [
          row.id_zendesk,
          row.assunto,
          mergedDescription,
          row.status,
          row.canal,
          requesterConsultant?.user_id || null,
          requesterId,
          requesterConsultant?.name || ('ID:' + requesterId),
          row.nome_cliente,
          row.email_cliente,
          row.tags,
          row.iniciado_por_ia,
          row.criado_no_zendesk,
          row.resolvido_no_zendesk
        ]);
        imported++;
      } catch (e) {
        console.warn('[PDCA IMPORT]', zid, e.message);
      }
    }

    res.json({ message: imported + ' ticket(s) importado(s)', imported });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pdca/tickets', authenticate, canManage, async (req, res) => {
  try {
    const params = [];
    const conditions = [];

    if (req.user.role === 'manager') {
      const managerRequesterIds = await getManagerRequesterIds(req.user.id);
      if (!managerRequesterIds?.length) return res.json({ tickets: [] });
      conditions.push(`id_assignee_zendesk = ANY($${params.length + 1})`);
      params.push(managerRequesterIds);
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const { rows } = await db.query(`
      SELECT *
      FROM pdca_tickets
      ${where}
      ORDER BY nome_consultor ASC, importado_em DESC
    `, params);

    const tickets = rows.map(t => ({
      id: t.id,
      zendesk_id: t.id_zendesk,
      subject: t.assunto,
      description: t.descricao,
      pdca_score: extractPdcaScore(t.descricao),
      consultant_name: t.nome_consultor,
      requester_name: t.nome_cliente,
      imported_at: t.importado_em,
      solved_at: t.resolvido_no_zendesk,
      tags: t.tags || [],
    }));
    res.json({ tickets });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pdca/tickets/:id', authenticate, canManage, async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM pdca_tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket PDCA nao encontrado' });
    const t = rows[0];

    if (req.user.role === 'manager') {
      const managerRequesterIds = await getManagerRequesterIds(req.user.id);
      const allowed = new Set((managerRequesterIds || []).map(String));
      if (!allowed.has(String(t.id_assignee_zendesk || ''))) {
        return res.status(403).json({ error: 'Acesso negado' });
      }
    }

    res.json({
      id: t.id,
      zendesk_id: t.id_zendesk,
      subject: t.assunto,
      description: t.descricao,
      pdca_score: extractPdcaScore(t.descricao),
      consultant_name: t.nome_consultor,
      requester_name: t.nome_cliente,
      solved_at: t.resolvido_no_zendesk,
      imported_at: t.importado_em,
      tags: t.tags || [],
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/tickets', authenticate, async (req, res) => {
  const { filter, page=1, limit=15, search } = req.query;
  try {
    const managerIds = await getManagerAgentIds(req.user.id);
    const conditions = ['tipo = $1'];
    const params = ['normal'];

    if (req.user.role === 'consultant') {
      conditions.push(`id_assignee_zendesk = ANY($${params.length+1})`);
      params.push(req.user.assignee_ids);
      conditions.push('descartado = false');
    } else if (managerIds) {
      conditions.push(`id_assignee_zendesk = ANY($${params.length+1})`);
      params.push(managerIds);
    }

    if (filter === 'pending')   { conditions.push('avaliado = false'); conditions.push('descartado = false'); }
    if (filter === 'evaluated') { conditions.push('avaliado = true'); }
    if (filter === 'discarded') { conditions.push('descartado = true'); }
    if (filter === 'ai')        { conditions.push('iniciado_por_ia = true'); conditions.push('descartado = false'); }

    if (search) {
      const s = `%${search}%`;
      conditions.push(`(id_zendesk ILIKE $${params.length+1} OR assunto ILIKE $${params.length+1} OR nome_cliente ILIKE $${params.length+1})`);
      params.push(s);
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
    const countRes = await db.query(`SELECT COUNT(*) FROM tickets ${where}`, params);
    const total = Number(countRes.rows[0].count);
    const offset = (Number(page)-1)*Number(limit);
    const { rows } = await db.query(`SELECT * FROM tickets ${where} ORDER BY importado_em DESC LIMIT $${params.length+1} OFFSET $${params.length+2}`, [...params, Number(limit), offset]);
    const tickets = rows.map(t => ({
      ...t,
      zendesk_id:         t.id_zendesk,
      subject:            t.assunto,
      consultant_name:    t.nome_consultor,
      requester_name:     t.nome_cliente,
      ai_initiated:       t.iniciado_por_ia,
      discarded:          t.descartado,
      evaluated:          t.avaliado,
      reevaluating:       t.em_reavaliacao,
      consultant_comment: t.comentario_consultor,
      consultant_reply:   t.resposta_consultor,
      received_at:        t.importado_em,
    }));
    res.json({ tickets, total, page: Number(page), pages: Math.ceil(total/limit)||1 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/tickets/:id', authenticate, async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const t = rows[0];
    if (req.user.role === 'consultant' && !req.user.assignee_ids.includes(t.id_assignee_zendesk))
      return res.status(403).json({ error: 'Acesso negado' });
    const { rows: evRows } = await db.query(
      'SELECT * FROM avaliacoes WHERE ticket_id = $1 ORDER BY criado_em DESC LIMIT 1', [t.id]
    );
    const ev = evRows[0];
    const ticket = {
      ...t,
      // aliases em inglês que o frontend usa
      zendesk_id:        t.id_zendesk,
      subject:           t.assunto,
      description:       t.descricao,
      consultant_name:   t.nome_consultor,
      requester_name:    t.nome_cliente,
      requester_email:   t.email_cliente,
      ai_initiated:      t.iniciado_por_ia,
      ai_analysis:       t.analise_ia,
      discarded:         t.descartado,
      evaluated:         t.avaliado,
      reevaluating:      t.em_reavaliacao,
      consultant_comment:t.comentario_consultor,
      consultant_reply:  t.resposta_consultor,
      received_at:       t.importado_em,
    };
    const evaluation = ev ? {
      ...ev,
      overall:        ev.nota_final,
      evaluator_name: ev.nome_avaliador,
      notes:          ev.observacoes,
      created_at:     ev.criado_em,
      scores: {
        solucao:   ev.nota_solucao,
        empatia:   ev.nota_empatia,
        conhecimento_produto:  ev.nota_conhecimento_produto,
      }
    } : null;
    res.json({ ticket, evaluation });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tickets/:id/discard', authenticate, canEvaluate, async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    if (rows[0].avaliado && !rows[0].em_reavaliacao) return res.status(400).json({ error: 'Ticket ja avaliado' });
    const ticket = rows[0];
    
    // Salva na tabela de descartados
    await db.query(`
      INSERT INTO tickets_descartados (ticket_id, id_zendesk, consultor_id, nome_consultor, descartado_por, descartado_em)
      VALUES ($1, $2, $3::uuid, $4, $5, NOW())
    `, [ticket.id, ticket.id_zendesk, ticket.consultor_id, ticket.nome_consultor, req.user.name]);
    
    // Marca ticket como descartado
    await db.query(`UPDATE tickets SET descartado = true, descartado_em = NOW(), descartado_por = $1 WHERE id = $2`, [req.user.name, req.params.id]);
    res.json({ message: 'Ticket descartado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tickets/:id/restore', authenticate, canEvaluate, async (req, res) => {
  try {
    await db.query(`UPDATE tickets SET descartado = false, descartado_em = NULL, descartado_por = NULL WHERE id = $1`, [req.params.id]);
    res.json({ message: 'Ticket restaurado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/zendesk/tickets/:zendesk_id/comments', authenticate, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  zendeskRequest('/api/v2/tickets/'+req.params.zendesk_id+'/comments.json', cfg)
    .then(d => res.json(d))
    .catch(e => res.status(500).json({ error: e.message }));
});

/* ══════════════════════════════════════════════════════════════
   IA: RESUMO + NOTA
══════════════════════════════════════════════════════════════ */

app.post('/api/tickets/:id/ai-review', authenticate, canEvaluate, async (req, res) => {
  try {
    const cfg = await getCfgForUser(req.user);
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    const force = req.query.force === '1';
    if (!force && ticket.analise_ia) return res.json({ ...ticket.analise_ia, cached: true });
    if (!cfg.ia_chave_api)       return res.status(400).json({ error: 'Chave Gemini nao configurada.' });
    if (!cfg.zendesk_subdominio) return res.status(400).json({ error: 'Zendesk nao configurado.' });

    const data     = await zendeskRequest('/api/v2/tickets/'+ticket.id_zendesk+'/comments.json', cfg);
    const conversa = (data.comments||[]).map(c => {
      const autor = c.author?.name || 'Participante';
      const texto = (c.plain_body||c.body||'').replace(/<[^>]*>/g,'').trim();
      return '['+new Date(c.created_at).toLocaleString('pt-BR')+'] '+autor+':\n'+texto;
    }).filter(Boolean).join('\n\n---\n\n');

    // Detecta rejeições mencionadas e busca documentação
    const rejeicoes = extractRejections(conversa);
    const documentacaoNaoEnviada = [];
    
    if (rejeicoes.length > 0) {
      for (const rejeicao of rejeicoes) {
        const artigos = await searchHelpCenterArticles('rejeição', rejeicao, cfg);
        
        if (artigos.length > 0) {
          // Verifica se algum dos links foi enviado pelo consultor na conversa
          const linksEnviados = artigos.some(artigo => 
            conversa.toLowerCase().includes(artigo.url.toLowerCase())
          );
          
          if (!linksEnviados) {
            documentacaoNaoEnviada.push({
              rejeicao: rejeicao,
              artigos: artigos.slice(0, 3) // Pega até 3 artigos mais relevantes
            });
          }
        }
      }
    }

    const docs   = cfg.documentacao_base ? '\n\nDOCUMENTACAO DE REFERENCIA:\n'+cfg.documentacao_base : '';
    
    // Adiciona informação sobre documentação não enviada no contexto da IA
    let docNaoEnviadaInfo = '';
    if (documentacaoNaoEnviada.length > 0) {
      docNaoEnviadaInfo = '\n\n⚠️ IMPORTANTE - DOCUMENTAÇÃO DISPONÍVEL NÃO ENVIADA:\n';
      docNaoEnviadaInfo += 'Identifiquei que havia documentação disponível na base de conhecimento que poderia ter sido enviada ao cliente:\n\n';
      
      documentacaoNaoEnviada.forEach(item => {
        docNaoEnviadaInfo += `Rejeição mencionada: "${item.rejeicao}"\n`;
        docNaoEnviadaInfo += 'Documentação disponível:\n';
        item.artigos.forEach(artigo => {
          docNaoEnviadaInfo += `- ${artigo.title}\n  Link: ${artigo.url}\n`;
        });
        docNaoEnviadaInfo += '\n';
      });
      
      docNaoEnviadaInfo += 'AÇÃO ESPERADA: O consultor deveria ter pesquisado e enviado esta documentação ao cliente para facilitar a resolução do problema.\n';
      docNaoEnviadaInfo += 'IMPACTO NA AVALIAÇÃO: Este deve ser considerado um PONTO DE MELHORIA significativo na análise, pois havia recursos disponíveis que não foram utilizados.\n';
    }
    
    const system = `Voce e um especialista em qualidade de atendimento ao cliente.
Analise a conversa e avalie em 3 criterios (escala 0,25,50,75,100):
- solucao: O agente resolveu o problema do cliente?
- empatia: O agente demonstrou empatia e compreensao?
- conhecimento_produto: O agente demonstrou conhecimento adequado do produto/serviço?

IMPORTANTE: Escreva TODA a análise em PRIMEIRA PESSOA, como se você fosse o avaliador.
Use "Eu avaliei...", "Observei que...", "Identifiquei que...", etc.

Responda APENAS em JSON valido sem markdown:
{"resumo":"...","pontos_positivos":["..."],"pontos_melhoria":["..."],"scores":{"solucao":75,"empatia":75,"conhecimento_produto":75},"nota_sugerida":75,"justificativa":"..."}${docs}${docNaoEnviadaInfo}`;

    const msg  = 'Ticket #'+ticket.id_zendesk+'\nConsultor: '+ticket.nome_consultor+'\nAssunto: '+ticket.assunto+'\n\nCONVERSA:\n'+conversa;
    const raw  = await callGemini(system, msg, cfg);
    const json = extractJSON(raw);
    await db.query('UPDATE tickets SET analise_ia = $1::jsonb, analise_ia_em = NOW() WHERE id = $2', [JSON.stringify(json), req.params.id]);
    res.json({ ...json, cached: false });
  } catch (e) { res.status(500).json({ error: 'Erro IA: '+e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   AVALIAR
══════════════════════════════════════════════════════════════ */

app.post('/api/tickets/:id/evaluate', authenticate, canEvaluate, async (req, res) => {
  const { scores, notes, overall, sendEmail } = req.body;
  if (!scores || overall === undefined) return res.status(400).json({ error: 'Dados incompletos' });
  // Converte escala 1-5 → 0,25,50,75,100
  const PTS = {1:0, 2:25, 3:50, 4:75, 5:100};
  const toDb = v => { const n = Number(v); return PTS[n] !== undefined ? PTS[n] : n; };
  try {
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    if (ticket.avaliado && !ticket.em_reavaliacao) return res.status(400).json({ error: 'Ticket ja avaliado' });

    // Salva avaliação anterior em histórico se for reavaliação
    if (ticket.em_reavaliacao) {
      const { rows: prevEvals } = await db.query(
        'SELECT * FROM avaliacoes WHERE ticket_id = $1 ORDER BY criado_em DESC LIMIT 1',
        [ticket.id]
      );
      if (prevEvals[0]) {
        const prev = prevEvals[0];
        await db.query(`
          INSERT INTO historico_reavaliacoes
            (ticket_id, avaliacao_id, avaliador_id, nome_avaliador,
             nota_solucao, nota_empatia, nota_conhecimento_produto, nota_final, observacoes, motivo)
          VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, 'Reavaliação solicitada')
        `, [
          ticket.id, prev.id, prev.avaliador_id, prev.nome_avaliador,
          prev.nota_solucao, prev.nota_empatia, prev.nota_conhecimento_produto,
          prev.nota_final, prev.observacoes || ''
        ]);
      }
      await db.query('DELETE FROM avaliacoes WHERE ticket_id = $1', [ticket.id]);
    }

    const { rows: evRows } = await db.query(`
      INSERT INTO avaliacoes (ticket_id, avaliador_id, nome_avaliador, id_assignee_zendesk, nome_consultor,
        nota_solucao, nota_empatia, nota_conhecimento_produto, nota_final, observacoes, origem, email_enviado)
      VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,'normal',false)
      RETURNING *
    `, [
      ticket.id, req.user.id, req.user.name,
      ticket.id_assignee_zendesk, ticket.nome_consultor,
      Number(toDb(scores.solucao)), Number(toDb(scores.empatia)), Number(toDb(scores.conhecimento_produto)),
      Number(overall), notes||''
    ]);
    const ev = evRows[0];

    await db.query('UPDATE tickets SET avaliado = true, em_reavaliacao = false WHERE id = $1', [ticket.id]);

    // Tag Zendesk - busca ticket completo e adiciona tag
    try {
      const cfg = await getCfgForUser(req.user);
      if (cfg.zendesk_subdominio && cfg.zendesk_token) {
        // Busca o ticket completo do Zendesk
        const zendeskTicket = await zendeskRequest(`/api/v2/tickets/${ticket.id_zendesk}.json`, cfg);
        const currentTags = zendeskTicket.ticket.tags || [];
        
        // Adiciona a tag se ainda não existir
        if (!currentTags.includes('ticket_avaliado')) {
          currentTags.push('ticket_avaliado');
        }
        
        // Atualiza o ticket com todas as tags
        await zendeskPut(`/api/v2/tickets/${ticket.id_zendesk}.json`, {
          ticket: {
            tags: currentTags
          }
        }, cfg);
      }
    } catch (tagErr) { console.warn('[ZENDESK TAG]', tagErr.message); }

    // E-mail para consultor
    if (sendEmail) {
      try {
        const cfg = await getCfgForUser(req.user);
        const { rows: consRows } = await db.query(`
          SELECT u.email, u.nome FROM usuarios u
          JOIN ids_atendente ia ON ia.usuario_id = u.id
          WHERE ia.id_zendesk = $1 LIMIT 1
        `, [ticket.id_assignee_zendesk]);
        if (consRows[0]?.email) {
          const SCORE_MAP = {1:{e:'😞',l:'Péssimo'},2:{e:'😕',l:'Ruim'},3:{e:'🙂',l:'Médio'},4:{e:'😊',l:'Bom'},5:{e:'🤩',l:'Ótimo'}};
          const toVal = n => n <= 0 ? 1 : n <= 25 ? 2 : n <= 50 ? 3 : n <= 75 ? 4 : 5;
          const scoreRow = (label, val, bg) => {
            const s = SCORE_MAP[toVal(val)] || {e:'–',l:'–'};
            return `<tr style="background:${bg}"><td style="padding:13px 18px;color:#93a8d4;font-size:.88rem;font-weight:500">${label}</td><td style="padding:13px 18px;text-align:right"><span style="font-size:1.3rem">${s.e}</span><span style="font-size:.88rem;margin-left:8px;color:#e8edf8;font-weight:600">${s.l}</span></td></tr>`;
          };
          const scoreColor = ev.nota_final>=75?'#3b82f6':ev.nota_final>=50?'#f59e0b':'#ef4444';
          await sendMail({
            to: consRows[0].email,
            subject: 'Avaliação de Atendimento — Ticket #'+ticket.id_zendesk,
            html: `<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:600px;margin:0 auto;background:#0b0e1a;border-radius:14px;overflow:hidden;border:1px solid #1e2d50">
  <div style="background:linear-gradient(135deg,#1d4ed8,#2563eb,#1e3a8a);padding:28px 32px 22px;text-align:center">
    <div style="color:#fff;font-size:1.3rem;font-weight:800;margin-bottom:4px">TecnoIT</div>
    <div style="color:rgba(255,255,255,.55);font-size:.7rem;letter-spacing:.1em;text-transform:uppercase">Plataforma TecnoIT · Atendimentos</div>
  </div>
  <div style="padding:30px 32px">
    <p style="margin:0 0 22px;font-size:.95rem;color:#c8d5ee;line-height:1.6">Olá, <strong style="color:#e8edf8">${ticket.nome_consultor}</strong>!<br>Seu atendimento ao ticket <strong style="color:#60a5fa">#${ticket.id_zendesk}</strong> foi avaliado.</p>
    <div style="background:#111627;border:1px solid #1e2d50;border-radius:12px;padding:22px;text-align:center;margin-bottom:22px">
      <div style="font-size:.68rem;color:#5b6e9a;text-transform:uppercase;letter-spacing:.12em;margin-bottom:8px">Nota Final</div>
      <div style="font-size:3.5rem;font-weight:800;color:${scoreColor};line-height:1;margin-bottom:4px">${ev.nota_final}</div>
      <div style="color:#5b6e9a;font-size:.8rem">de 100 pontos</div>
    </div>
    <table style="width:100%;border-collapse:collapse;border-radius:10px;overflow:hidden;border:1px solid #1e2d50">
      <thead><tr style="background:#131620"><th style="padding:10px 18px;text-align:left;font-size:.68rem;color:#5b6e9a;text-transform:uppercase;font-weight:600">Critério</th><th style="padding:10px 18px;text-align:right;font-size:.68rem;color:#5b6e9a;text-transform:uppercase;font-weight:600">Resultado</th></tr></thead>
      <tbody>${scoreRow('Solução',ev.nota_solucao,'#0d0f18')}${scoreRow('Empatia',ev.nota_empatia,'#111627')}${scoreRow('Conhecimento do Produto',ev.nota_conhecimento_produto,'#0d0f18')}</tbody>
    </table>
    ${ev.observacoes ? `<div style="margin-top:20px;background:#111627;border-left:3px solid #2563eb;border-radius:0 10px 10px 0;padding:16px 18px"><div style="font-size:.68rem;color:#3b82f6;text-transform:uppercase;font-weight:700;margin-bottom:7px">📝 Observação do Avaliador</div><p style="margin:0;font-size:.88rem;color:#c8d5ee;line-height:1.65">${ev.observacoes}</p></div>` : ''}
    <div style="margin-top:24px;padding-top:16px;border-top:1px solid #1e2d50"><p style="margin:0;font-size:.76rem;color:#5b6e9a">Avaliado por <strong style="color:#93a8d4">${ev.nome_avaliador}</strong> em ${new Date(ev.criado_em).toLocaleString('pt-BR')}</p></div>
  </div>
</div>`
          }, cfg);
          await db.query('UPDATE avaliacoes SET email_enviado = true WHERE id = $1', [ev.id]);
        }
      } catch (e) { console.warn('[EMAIL]', e.message); }
    }

    res.status(201).json({ message: 'Avaliacao salva', evaluation: ev });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tickets/:id/reevaluate', authenticate, canEvaluate, async (req, res) => {
  try {
    const { rowCount } = await db.query(
      'UPDATE tickets SET em_reavaliacao = true WHERE id = $1 AND avaliado = true',
      [req.params.id]
    );
    if (!rowCount) return res.status(400).json({ error: 'Ticket nao avaliado ainda' });
    res.json({ message: 'Reavaliação habilitada' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tickets/:id/comment', authenticate, async (req, res) => {
  const { comment } = req.body;
  if (!comment?.trim()) return res.status(400).json({ error: 'Comentario vazio' });
  try {
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    if (req.user.role === 'consultant' && !req.user.assignee_ids.includes(rows[0].id_assignee_zendesk))
      return res.status(403).json({ error: 'Acesso negado' });
    await db.query('UPDATE tickets SET comentario_consultor = $1, comentario_em = NOW() WHERE id = $2', [comment.trim(), req.params.id]);
    res.json({ message: 'Comentario salvo' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/tickets/:id/reply', authenticate, async (req, res) => {
  const { reply } = req.body;
  if (!reply?.trim()) return res.status(400).json({ error: 'Resposta vazia' });
  try {
    const { rows } = await db.query('SELECT * FROM tickets WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    if (req.user.role === 'consultant' && !req.user.assignee_ids.includes(ticket.id_assignee_zendesk))
      return res.status(403).json({ error: 'Acesso negado' });
    if (!ticket.avaliado)           return res.status(400).json({ error: 'Ticket nao avaliado' });
    if (ticket.resposta_consultor)  return res.status(400).json({ error: 'Ja respondeu' });
    await db.query('UPDATE tickets SET resposta_consultor = $1, resposta_em = NOW() WHERE id = $2', [reply.trim(), req.params.id]);

    // Notifica avaliador
    try {
      const cfg = await getCfgForUser(req.user);
      const { rows: evRows } = await db.query(
        'SELECT * FROM avaliacoes WHERE ticket_id = $1 ORDER BY criado_em DESC LIMIT 1',
        [ticket.id]
      );
      if (evRows[0]) {
        const { rows: evalUserRows } = await db.query('SELECT * FROM usuarios WHERE id = $1', [evRows[0].avaliador_id]);
        if (evalUserRows[0]?.email) {
          const ev = evRows[0], evaluator = evalUserRows[0];
          await sendMail({
            to: evaluator.email,
            subject: 'Consultor respondeu — Ticket #' + ticket.id_zendesk,
            html: `<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:580px;margin:0 auto;background:#0b0e1a;border-radius:14px;overflow:hidden;border:1px solid #1e2d50">
  <div style="background:linear-gradient(135deg,#1d4ed8,#2563eb,#1e3a8a);padding:24px 30px 20px;text-align:center">
    <div style="color:#fff;font-size:1.2rem;font-weight:800;margin-bottom:4px">💬 Nova Resposta do Consultor</div>
    <div style="color:rgba(255,255,255,.55);font-size:.7rem;letter-spacing:.1em;text-transform:uppercase">Plataforma TecnoIT · Atendimentos</div>
  </div>
  <div style="padding:26px 30px">
    <p style="margin:0 0 18px;font-size:.93rem;color:#c8d5ee;line-height:1.6">Olá, <strong style="color:#e8edf8">${evaluator.nome}</strong>!<br>O consultor <strong style="color:#60a5fa">${ticket.nome_consultor}</strong> respondeu à avaliação do ticket <strong style="color:#60a5fa">#${ticket.id_zendesk}</strong>.</p>
    <div style="background:#111627;border:1px solid #1e2d50;border-radius:10px;padding:16px 18px;margin-bottom:20px">
      <div style="font-size:.67rem;color:#5b6e9a;text-transform:uppercase;font-weight:600;margin-bottom:8px">Resposta do Consultor</div>
      <p style="margin:0;font-size:.9rem;color:#e8edf8;line-height:1.65;white-space:pre-wrap">${reply.trim()}</p>
    </div>
    <div style="background:#111627;border:1px solid #1e2d50;border-radius:10px;padding:14px 18px;margin-bottom:20px">
      <div style="font-size:.67rem;color:#5b6e9a;text-transform:uppercase;margin-bottom:4px">Nota que você deu</div>
      <div style="font-size:1.6rem;font-weight:800;color:${ev.nota_final>=75?'#3b82f6':ev.nota_final>=50?'#f59e0b':'#ef4444'}">${ev.nota_final}<span style="font-size:.85rem;color:#5b6e9a;font-weight:400"> / 100</span></div>
    </div>
    <div style="margin-top:16px;padding-top:14px;border-top:1px solid #1e2d50;font-size:.75rem;color:#5b6e9a">Acesse a plataforma para rever a avaliação ou reavaliar o atendimento.</div>
  </div>
</div>`
          }, cfg);
        }
      }
    } catch(mailErr) { console.warn('[EMAIL REPLY]', mailErr.message); }
    res.json({ message: 'Resposta salva' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   CSAT NEGATIVOS
══════════════════════════════════════════════════════════════ */

app.get('/api/csat/import', authenticate, canEvaluate, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { year, month, group_ids, consultant_ids } = req.query;
  if (!year || !month) return res.status(400).json({ error: 'Informe ano e mes' });
  const y = Number(year), m = Number(month);
  const start = new Date(y, m-1, 1).toISOString().slice(0,10);
  const end   = new Date(y, m, 1).toISOString().slice(0,10);
  try {
    const selectedGroupIds = parseCsvQueryIds(group_ids);
    const selectedConsultantIds = parseCsvQueryIds(consultant_ids);
    if (selectedConsultantIds.length && !selectedGroupIds.length) {
      return res.status(400).json({ error: 'Selecione ao menos um grupo para filtrar por consultor.' });
    }

    let allowedAssigneeSet = null;
    if (selectedGroupIds.length) {
      const scope = await loadConsultantsScopeForUser(req.user, selectedGroupIds.join(','));
      const groupConsultants = scope.consultants || [];
      const consultantIdSet = new Set(groupConsultants.map(c => String(c.id)));
      const invalidConsultant = selectedConsultantIds.find(id => !consultantIdSet.has(String(id)));
      if (invalidConsultant) {
        return res.status(403).json({ error: 'Consultor invalido para o(s) grupo(s) selecionado(s).' });
      }
      const effectiveConsultants = selectedConsultantIds.length
        ? groupConsultants.filter(c => selectedConsultantIds.includes(String(c.id)))
        : groupConsultants;
      const assigneeIds = [...new Set(
        effectiveConsultants.flatMap(c => c.assignee_ids || []).map(v => String(v)).filter(Boolean)
      )];
      allowedAssigneeSet = new Set(assigneeIds);
      if (!allowedAssigneeSet.size) {
        return res.json({ tickets: [], total: 0 });
      }
    }

    const q    = encodeURIComponent('type:ticket satisfaction:bad updated>'+start+' updated<'+end);
    const data = await zendeskRequest('/api/v2/search.json?query='+q+'&per_page=100', cfg, 'IMPORT_CSAT_NEGATIVOS');
    const amap = await getAssigneeMap();
    const { rows: existingRows } = await db.query("SELECT id_zendesk FROM tickets WHERE tipo = 'csat'");
    const existing = new Set(existingRows.map(r => r.id_zendesk));
    const tickets = (data.results||[])
      .filter(t => {
        if (!allowedAssigneeSet) return true;
        return allowedAssigneeSet.has(String(t.assignee_id || ''));
      })
      .map(t => ({
      id: String(t.id), zendesk_id: String(t.id),
      subject: t.subject||'Sem assunto',
      requester_name: t.via?.source?.from?.name || t.requester?.name || 'Cliente',
      assignee_id: String(t.assignee_id||''),
      consultant_name: amap[String(t.assignee_id)] || 'ID:'+t.assignee_id,
      satisfaction: t.satisfaction_rating?.score || 'bad',
      satisfaction_comment: t.satisfaction_rating?.comment || '',
      solved_at: t.updated_at,
      ai_initiated: (t.tags || []).includes('claudia_escalado_n2'),
      already_imported: existing.has(String(t.id))
    }));
    res.json({ tickets, total: tickets.length });
  } catch(e) { res.status(500).json({ error: 'Erro Zendesk: '+e.message }); }
});

app.post('/api/csat/import', authenticate, canEvaluate, async (req, res) => {
  const cfg = await getCfgForUser(req.user);
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { ticket_ids } = req.body;
  if (!ticket_ids?.length) return res.status(400).json({ error: 'Nenhum ticket selecionado' });
  const { rows: existingRows } = await db.query("SELECT id_zendesk FROM tickets WHERE tipo = 'csat'");
  const existing = new Set(existingRows.map(r => r.id_zendesk));
  const amap = await getAssigneeMap();
  let imported = 0;
  for (const zid of ticket_ids) {
    if (existing.has(String(zid))) continue;
    try {
      const d = await zendeskRequest('/api/v2/tickets/'+zid+'.json', cfg);
      const t = d.ticket;
      const row = await buildTicketRow(String(t.id), String(t.assignee_id||''), t, t, 'csat', amap);
      await db.query(`
        INSERT INTO tickets (id_zendesk,tipo,assunto,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,nome_cliente,tags,iniciado_por_ia,nota_csat,comentario_csat,resolvido_no_zendesk)
        VALUES ($1,'csat',$2,'resolvido',$3,$4::uuid,$5,$6,$7,$8,$9,$10::varchar,$11::varchar,$12::timestamptz)
        ON CONFLICT (id_zendesk) DO NOTHING
      `, [
        row.id_zendesk, row.assunto, row.canal, row.consultor_id, row.id_assignee_zendesk,
        row.nome_consultor, row.nome_cliente, row.tags, row.iniciado_por_ia,
        t.satisfaction_rating?.score || 'bad',
        t.satisfaction_rating?.comment || null,
        t.updated_at || null,
      ]);
      imported++;
    } catch (e) { console.warn('[CSAT IMPORT]', zid, e.message); }
  }
  res.json({ message: imported+' ticket(s) importado(s)', imported });
});

app.get('/api/csat/tickets', authenticate, async (req, res) => {
  const { filter, page=1, limit=15, search } = req.query;
  try {
    const managerIds = await getManagerAgentIds(req.user.id);
    const conditions = ["tipo = 'csat'"];
    const params = [];

    // Filtro por consultor
    if (req.user.role === 'consultant') {
      conditions.push(`id_assignee_zendesk = ANY($${params.length+1})`);
      params.push(req.user.assignee_ids);
    } else if (managerIds) {
      conditions.push(`id_assignee_zendesk = ANY($${params.length+1})`);
      params.push(managerIds);
    }

    if (filter === 'pending')   { conditions.push('avaliado = false'); conditions.push('descartado = false'); }
    if (filter === 'evaluated') { conditions.push('avaliado = true'); }
    if (filter === 'discarded') { conditions.push('descartado = true'); }

    if (search) {
      const s = `%${search}%`;
      conditions.push(`(id_zendesk ILIKE $${params.length+1} OR assunto ILIKE $${params.length+1} OR nome_cliente ILIKE $${params.length+1})`);
      params.push(s);
    }

    const where  = 'WHERE ' + conditions.join(' AND ');
    const count  = await db.query(`SELECT COUNT(*) FROM tickets ${where}`, params);
    const total  = Number(count.rows[0].count);
    const offset = (Number(page)-1)*Number(limit);
    const { rows } = await db.query(`SELECT * FROM tickets ${where} ORDER BY importado_em DESC LIMIT $${params.length+1} OFFSET $${params.length+2}`, [...params, Number(limit), offset]);
    const tickets = rows.map(t => ({
      ...t,
      zendesk_id:          t.id_zendesk,
      subject:             t.assunto,
      consultant_name:     t.nome_consultor,
      requester_name:      t.nome_cliente,
      satisfaction_rating: t.nota_csat,
      satisfaction_comment:t.comentario_csat,
      ai_initiated:        t.iniciado_por_ia,
      discarded:           t.descartado,
      evaluated:           t.avaliado,
      received_at:         t.importado_em,
    }));
    res.json({ tickets, total, page: Number(page), pages: Math.ceil(total/limit)||1 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/csat/tickets/:id', authenticate, async (req, res) => {
  try {
    const { rows } = await db.query("SELECT * FROM tickets WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const t = rows[0];
    
    // Verifica permissão de acesso
    if (req.user.role === 'consultant' && !req.user.assignee_ids.includes(t.id_assignee_zendesk)) {
      return res.status(403).json({ error: 'Acesso negado' });
    }
    
    const { rows: evRows } = await db.query('SELECT * FROM avaliacoes WHERE ticket_id = $1 ORDER BY criado_em DESC LIMIT 1', [t.id]);
    const ev = evRows[0];
    const ticket = {
      ...t,
      zendesk_id:          t.id_zendesk,
      subject:             t.assunto,
      description:         t.descricao,
      consultant_name:     t.nome_consultor,
      requester_name:      t.nome_cliente,
      requester_email:     t.email_cliente,
      satisfaction_rating: t.nota_csat,
      satisfaction_comment:t.comentario_csat,
      ai_initiated:        t.iniciado_por_ia,
      discarded:           t.descartado,
      evaluated:           t.avaliado,
    };
    const evaluation = ev ? {
      ...ev,
      overall:        ev.nota_final,
      evaluator_name: ev.nome_avaliador,
      notes:          ev.observacoes,
      created_at:     ev.criado_em,
      scores: {
        solucao:  ev.nota_solucao,
        empatia:  ev.nota_empatia,
        conhecimento_produto: ev.nota_conhecimento_produto,
      }
    } : null;
    res.json({ ticket, evaluation });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/csat/tickets/:id/evaluate', authenticate, canEvaluate, async (req, res) => {
  const { scores, notes, overall, notCountInNegativados } = req.body;
  if (!scores || overall === undefined) return res.status(400).json({ error: 'Dados incompletos' });
  const PTS2 = {1:0, 2:25, 3:50, 4:75, 5:100};
  const toDb2 = v => { const n = Number(v); return PTS2[n] !== undefined ? PTS2[n] : n; };
  const noTotalizarNegativado = !!notCountInNegativados;
  try {
    const { rows } = await db.query("SELECT * FROM tickets WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    if (ticket.avaliado && !ticket.em_reavaliacao) return res.status(400).json({ error: 'Ticket ja avaliado' });
    await db.query(`
      INSERT INTO avaliacoes (ticket_id,avaliador_id,nome_avaliador,id_assignee_zendesk,nome_consultor,nota_solucao,nota_empatia,nota_conhecimento_produto,nota_final,observacoes,origem,email_enviado,nao_totalizar_negativado)
      VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,'csat',false,$11)
    `, [ticket.id,req.user.id,req.user.name,ticket.id_assignee_zendesk,ticket.nome_consultor,Number(toDb2(scores.solucao)),Number(toDb2(scores.empatia)),Number(toDb2(scores.conhecimento_produto)),Number(overall),notes||'',noTotalizarNegativado]);
    await db.query('UPDATE tickets SET avaliado = true, em_reavaliacao = false WHERE id = $1', [ticket.id]);
    // Tags Zendesk - busca ticket completo e adiciona tags
    try {
      const cfg = await getCfgForUser(req.user);
      if (cfg.zendesk_subdominio && cfg.zendesk_token) {
        // Busca o ticket completo do Zendesk
        const zendeskTicket = await zendeskRequest(`/api/v2/tickets/${ticket.id_zendesk}.json`, cfg);
        const currentTags = zendeskTicket.ticket.tags || [];
        
        // Adiciona as tags se ainda não existirem
        const tagsToAdd = ['ticket_avaliado', 'csat_negativo_avaliado'];
        tagsToAdd.forEach(tag => {
          if (!currentTags.includes(tag)) {
            currentTags.push(tag);
          }
        });
        
        // Atualiza o ticket com todas as tags
        await zendeskPut(`/api/v2/tickets/${ticket.id_zendesk}.json`, {
          ticket: {
            tags: currentTags
          }
        }, cfg);
      }
    } catch (tagErr) { console.warn('[ZENDESK TAG CSAT]', tagErr.message); }
    res.status(201).json({ message: 'Avaliacao CSAT salva' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/csat/tickets/:id/discard', authenticate, canEvaluate, async (req, res) => {
  try {
    await db.query("UPDATE tickets SET descartado = true, descartado_em = NOW(), descartado_por = $1 WHERE id = $2 AND tipo = 'csat'", [req.user.name, req.params.id]);
    res.json({ message: 'Descartado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/csat/tickets/:id/restore', authenticate, canEvaluate, async (req, res) => {
  try {
    await db.query("UPDATE tickets SET descartado = false, descartado_em = NULL, descartado_por = NULL WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    res.json({ message: 'Restaurado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/csat/tickets/:id/ai-review', authenticate, canEvaluate, async (req, res) => {
  try {
    const cfg = await getCfgForUser(req.user);
    const { rows } = await db.query("SELECT * FROM tickets WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    const force = req.query.force === '1';
    if (!force && ticket.analise_ia) return res.json({ ...ticket.analise_ia, cached: true });
    if (!cfg.ia_chave_api)       return res.status(400).json({ error: 'Chave Gemini nao configurada.' });
    if (!cfg.zendesk_subdominio) return res.status(400).json({ error: 'Zendesk nao configurado.' });
    const data     = await zendeskRequest('/api/v2/tickets/'+ticket.id_zendesk+'/comments.json', cfg);
    const conversa = (data.comments||[]).map(c => {
      const autor = c.author?.name||'Participante';
      const texto = (c.plain_body||c.body||'').replace(/<[^>]*>/g,'').trim();
      return '['+new Date(c.created_at).toLocaleString('pt-BR')+'] '+autor+':\n'+texto;
    }).filter(Boolean).join('\n\n---\n\n');
    
    // Detecta rejeições mencionadas e busca documentação
    const rejeicoes = extractRejections(conversa);
    const documentacaoNaoEnviada = [];
    
    if (rejeicoes.length > 0) {
      for (const rejeicao of rejeicoes) {
        const artigos = await searchHelpCenterArticles('rejeição', rejeicao, cfg);
        
        if (artigos.length > 0) {
          // Verifica se algum dos links foi enviado pelo consultor na conversa
          const linksEnviados = artigos.some(artigo => 
            conversa.toLowerCase().includes(artigo.url.toLowerCase())
          );
          
          if (!linksEnviados) {
            documentacaoNaoEnviada.push({
              rejeicao: rejeicao,
              artigos: artigos.slice(0, 3) // Pega até 3 artigos mais relevantes
            });
          }
        }
      }
    }
    
    const docs   = cfg.documentacao_base ? '\n\nDOCUMENTACAO DE REFERENCIA:\n'+cfg.documentacao_base : '';
    
    // Adiciona informação sobre documentação não enviada no contexto da IA
    let docNaoEnviadaInfo = '';
    if (documentacaoNaoEnviada.length > 0) {
      docNaoEnviadaInfo = '\n\n⚠️ IMPORTANTE - DOCUMENTAÇÃO DISPONÍVEL NÃO ENVIADA:\n';
      docNaoEnviadaInfo += 'Identifiquei que havia documentação disponível na base de conhecimento que poderia ter sido enviada ao cliente:\n\n';
      
      documentacaoNaoEnviada.forEach(item => {
        docNaoEnviadaInfo += `Rejeição mencionada: "${item.rejeicao}"\n`;
        docNaoEnviadaInfo += 'Documentação disponível:\n';
        item.artigos.forEach(artigo => {
          docNaoEnviadaInfo += `- ${artigo.title}\n  Link: ${artigo.url}\n`;
        });
        docNaoEnviadaInfo += '\n';
      });
      
      docNaoEnviadaInfo += 'AÇÃO ESPERADA: O consultor deveria ter pesquisado e enviado esta documentação ao cliente para facilitar a resolução do problema.\n';
      docNaoEnviadaInfo += 'IMPACTO NA AVALIAÇÃO: Este deve ser considerado um PONTO DE MELHORIA significativo na análise, pois havia recursos disponíveis que não foram utilizados. Isso pode ter contribuído para a insatisfação do cliente.\n';
    }
    
    const system = 'Voce e um especialista em qualidade de atendimento.\nEste ticket recebeu uma AVALIACAO NEGATIVA do cliente (CSAT ruim). Analise o motivo da insatisfacao e avalie em 3 criterios (0,25,50,75,100): solucao, empatia, conhecimento_produto.\n\nIMPORTANTE: Escreva TODA a análise em PRIMEIRA PESSOA, como se você fosse o avaliador.\nUse "Eu avaliei...", "Observei que...", "Identifiquei que...", etc.\n\nResponda APENAS em JSON valido sem markdown:\n{"resumo":"...","pontos_positivos":["..."],"pontos_melhoria":["..."],"motivo_insatisfacao":"...","scores":{"solucao":50,"empatia":50,"conhecimento_produto":50},"nota_sugerida":50,"justificativa":"..."}'+docs+docNaoEnviadaInfo;
    const msg    = 'Ticket #'+ticket.id_zendesk+'\nConsultor: '+ticket.nome_consultor+'\nFeedback do cliente: '+(ticket.comentario_csat||'(sem comentário)')+'\n\nCONVERSA:\n'+conversa;
    const raw    = await callGemini(system, msg, cfg);
    const json   = extractJSON(raw);
    await db.query('UPDATE tickets SET analise_ia = $1, analise_ia_em = NOW() WHERE id = $2', [json, req.params.id]);
    res.json({ ...json, cached: false });
  } catch(e) { res.status(500).json({ error: 'Erro IA: '+e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   STATS / EVALUATIONS
══════════════════════════════════════════════════════════════ */

app.get('/api/evaluations', authenticate, async (req, res) => {
  try {
    let q = `
      SELECT 
        a.*,
        t.id_zendesk AS zendesk_id,
        t.assunto AS subject
      FROM avaliacoes a
      LEFT JOIN tickets t ON t.id = a.ticket_id
    `;
    const params = [];
    if (req.user.role === 'consultant') {
      q += ' WHERE a.id_assignee_zendesk = ANY($1)';
      params.push(req.user.assignee_ids);
    }
    q += ' ORDER BY a.criado_em DESC';
    const { rows } = await db.query(q, params);
    
    // Mapeia os dados para os aliases que o frontend espera
    const evaluations = rows.map(ev => ({
      ...ev,
      overall: ev.nota_final,
      evaluator_name: ev.nome_avaliador,
      consultant_name: ev.nome_consultor,
      notes: ev.observacoes,
      created_at: ev.criado_em,
      scores: {
        solucao: ev.nota_solucao,
        empatia: ev.nota_empatia,
        conhecimento_produto: ev.nota_conhecimento_produto,
      }
    }));
    
    res.json({ evaluations });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stats', authenticate, async (req, res) => {
  try {
    const managerIds = await getManagerAgentIds(req.user.id);
    let assigneeFilter = '';
    const params = [];

    if (req.user.role === 'consultant') {
      assigneeFilter = `AND id_assignee_zendesk = ANY($${params.length+1})`;
      params.push(req.user.assignee_ids);
    } else if (managerIds) {
      assigneeFilter = `AND id_assignee_zendesk = ANY($${params.length+1})`;
      params.push(managerIds);
    }

    const ticketQ = await db.query(`
      SELECT
        COUNT(*) FILTER (WHERE NOT descartado AND tipo='normal')        AS total,
        COUNT(*) FILTER (WHERE descartado     AND tipo='normal')        AS descartados,
        COUNT(*) FILTER (WHERE avaliado       AND tipo='normal')        AS avaliados,
        COUNT(*) FILTER (WHERE NOT avaliado AND NOT descartado AND tipo='normal') AS pendentes
      FROM tickets WHERE tipo='normal' ${assigneeFilter}
    `, params);

    const evalQ = await db.query(`
      SELECT
        COUNT(*)               AS total_evals,
        ROUND(AVG(nota_solucao))  AS avg_solucao,
        ROUND(AVG(nota_empatia))  AS avg_empatia,
        ROUND(AVG(nota_conhecimento_produto)) AS avg_conhecimento_produto,
        ROUND(AVG(nota_final))    AS avg_final
      FROM avaliacoes
      WHERE origem = 'normal' ${assigneeFilter.replace('id_assignee_zendesk', 'id_assignee_zendesk')}
    `, params);

    const stats = ticketQ.rows[0];
    const evalStats = evalQ.rows[0];

    let ranking = [], quotaByConsultant = {};
    if (req.user.role !== 'consultant') {
      const rankQ = await db.query(`
        SELECT nome_consultor, ROUND(AVG(nota_final)) AS avg, COUNT(*) AS count
        FROM avaliacoes WHERE origem = 'normal' ${assigneeFilter}
        GROUP BY nome_consultor ORDER BY avg DESC
      `, params);
      ranking = rankQ.rows.map(r => ({ name: r.nome_consultor, avg: Number(r.avg), count: Number(r.count) }));
      ranking.forEach(r => { quotaByConsultant[r.name] = r.count; });
    }

    res.json({
      totalTickets:    Number(stats.total),
      evaluated:       Number(stats.avaliados),
      pending:         Number(stats.pendentes),
      discarded:       Number(stats.descartados),
      totalEvaluations:Number(evalStats.total_evals),
      avgScores: {
        solucao:  Number(evalStats.avg_solucao  || 0),
        empatia:  Number(evalStats.avg_empatia  || 0),
        conhecimento_produto: Number(evalStats.avg_conhecimento_produto || 0),
      },
      avgOverall: Number(evalStats.avg_final || 0),
      ranking,
      quotaByConsultant,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/simulate-ticket', authenticate, canEvaluate, async (req, res) => {
  const { assignee_id, subject, requester_name } = req.body;
  if (!assignee_id) return res.status(400).json({ error: 'assignee_id obrigatorio' });
  try {
    const amap = await getAssigneeMap();
    const zid  = 'SIM-'+Date.now();
    const row  = await buildTicketRow(zid, String(assignee_id), {
      subject: subject||'Ticket simulado #'+zid,
      description: 'Simulado.',
      status: 'resolvido',
      requester_name: requester_name||'Cliente Teste',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }, {}, 'normal', amap);
    const { rows } = await db.query(`
      INSERT INTO tickets (id_zendesk,tipo,assunto,descricao,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,nome_cliente)
      VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9,$10)
      RETURNING *
    `, [row.id_zendesk,'normal',row.assunto,row.descricao,row.status,row.canal,row.consultor_id,row.id_assignee_zendesk,row.nome_consultor,row.nome_cliente]);
    res.status(201).json({ message: 'Ticket simulado criado', ticket: rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   AUTO-INIT + START
══════════════════════════════════════════════════════════════ */

async function autoInit() {
  // Cria tabelas essenciais diretamente (sem depender do migrate.sql completo)
  await db.query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`);

  await db.query(`CREATE TABLE IF NOT EXISTS usuarios (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    login VARCHAR(100) UNIQUE NOT NULL,
    nome VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    foto_url TEXT,
    senha_hash VARCHAR(200) NOT NULL,
    papel VARCHAR(20) NOT NULL DEFAULT 'consultant' CHECK (papel IN ('consultant','evaluator','manager','admin')),
    tipo_usuario VARCHAR(30),
    id_zendesk VARCHAR(50) UNIQUE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);
  await db.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS foto_url TEXT`);
  await db.query(`ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS tipo_usuario VARCHAR(30)`);

  await db.query(`CREATE TABLE IF NOT EXISTS grupos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_zendesk VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(200) UNIQUE NOT NULL,
    sincronizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  await db.query(`CREATE TABLE IF NOT EXISTS ids_atendente (
    usuario_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    id_zendesk VARCHAR(50) NOT NULL,
    PRIMARY KEY (usuario_id, id_zendesk)
  )`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_ids_atendente_zendesk ON ids_atendente(id_zendesk)`);

  await db.query(`CREATE TABLE IF NOT EXISTS membros_grupo (
    usuario_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    grupo_id UUID NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
    PRIMARY KEY (usuario_id, grupo_id)
  )`);

  await db.query(`CREATE TABLE IF NOT EXISTS grupo_agrupador_vinculos (
    agrupador_id UUID NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
    grupo_id UUID NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
    PRIMARY KEY (agrupador_id, grupo_id),
    CHECK (agrupador_id <> grupo_id)
  )`);

  await db.query(`CREATE TABLE IF NOT EXISTS responsaveis_grupo (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    grupo_id UUID NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
    usuario_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    atribuido_em TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (grupo_id, usuario_id)
  )`);

  await db.query(`CREATE TABLE IF NOT EXISTS tickets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_zendesk VARCHAR(100) UNIQUE NOT NULL,
    tipo VARCHAR(10) NOT NULL DEFAULT 'normal' CHECK (tipo IN ('normal','csat')),
    assunto VARCHAR(500),
    descricao TEXT,
    status VARCHAR(50) DEFAULT 'resolvido',
    canal VARCHAR(50) DEFAULT 'web',
    consultor_id UUID REFERENCES usuarios(id),
    id_assignee_zendesk VARCHAR(50),
    nome_consultor VARCHAR(200),
    nome_cliente VARCHAR(200),
    email_cliente VARCHAR(200),
    tags TEXT[] DEFAULT '{}',
    iniciado_por_ia BOOLEAN DEFAULT FALSE,
    avaliado BOOLEAN DEFAULT FALSE,
    descartado BOOLEAN DEFAULT FALSE,
    em_reavaliacao BOOLEAN DEFAULT FALSE,
    nota_csat VARCHAR(20),
    comentario_csat TEXT,
    analise_ia JSONB,
    analise_ia_em TIMESTAMPTZ,
    descartado_em TIMESTAMPTZ,
    descartado_por VARCHAR(200),
    comentario_consultor TEXT,
    comentario_em TIMESTAMPTZ,
    resposta_consultor TEXT,
    resposta_em TIMESTAMPTZ,
    criado_no_zendesk TIMESTAMPTZ,
    resolvido_no_zendesk TIMESTAMPTZ,
    importado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_tickets_tipo ON tickets(tipo)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_tickets_assignee ON tickets(id_assignee_zendesk)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_tickets_avaliado ON tickets(avaliado, descartado)`);

  await db.query(`CREATE TABLE IF NOT EXISTS pdca_tickets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_zendesk VARCHAR(100) UNIQUE NOT NULL,
    assunto VARCHAR(500),
    descricao TEXT,
    status VARCHAR(50) DEFAULT 'resolvido',
    canal VARCHAR(50) DEFAULT 'web',
    consultor_id UUID REFERENCES usuarios(id),
    id_assignee_zendesk VARCHAR(50),
    nome_consultor VARCHAR(200),
    nome_cliente VARCHAR(200),
    email_cliente VARCHAR(200),
    tags TEXT[] DEFAULT '{}',
    iniciado_por_ia BOOLEAN DEFAULT FALSE,
    criado_no_zendesk TIMESTAMPTZ,
    resolvido_no_zendesk TIMESTAMPTZ,
    importado_em TIMESTAMPTZ DEFAULT NOW()
  )`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_pdca_tickets_assignee ON pdca_tickets(id_assignee_zendesk)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_pdca_tickets_importado ON pdca_tickets(importado_em)`);

  await db.query(`CREATE TABLE IF NOT EXISTS avaliacoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    avaliador_id UUID REFERENCES usuarios(id),
    nome_avaliador VARCHAR(200),
    id_assignee_zendesk VARCHAR(50),
    nome_consultor VARCHAR(200),
    nota_solucao SMALLINT CHECK (nota_solucao IN (0,25,50,75,100)),
    nota_empatia SMALLINT CHECK (nota_empatia IN (0,25,50,75,100)),
    nota_conhecimento_produto SMALLINT CHECK (nota_conhecimento_produto IN (0,25,50,75,100)),
    nota_final SMALLINT,
    observacoes TEXT,
    origem VARCHAR(10) DEFAULT 'normal' CHECK (origem IN ('normal','csat')),
    email_enviado BOOLEAN NOT NULL DEFAULT FALSE,
    nao_totalizar_negativado BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Adiciona coluna email_enviado se a tabela já existia sem ela
  await db.query(`ALTER TABLE avaliacoes ADD COLUMN IF NOT EXISTS email_enviado BOOLEAN NOT NULL DEFAULT FALSE`);;
  await db.query(`ALTER TABLE avaliacoes ADD COLUMN IF NOT EXISTS nao_totalizar_negativado BOOLEAN NOT NULL DEFAULT FALSE`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_avaliacoes_ticket ON avaliacoes(ticket_id)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_avaliacoes_assignee ON avaliacoes(id_assignee_zendesk)`);

  await db.query(`CREATE TABLE IF NOT EXISTS historico_csat_consultor (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consultor_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    mes_ref DATE NOT NULL,
    csat_percent NUMERIC(5,2),
    total_avaliacoes INTEGER NOT NULL DEFAULT 0,
    positivos INTEGER NOT NULL DEFAULT 0,
    negativos INTEGER NOT NULL DEFAULT 0,
    fonte VARCHAR(30) NOT NULL DEFAULT 'zendesk',
    erro TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (consultor_id, mes_ref)
  )`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_hist_csat_mes_ref ON historico_csat_consultor(mes_ref)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_hist_csat_consultor ON historico_csat_consultor(consultor_id)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS atendimento_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS atendimento_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS negativados_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS negativados_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS pdca_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS pdca_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS total_atendimentos INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS fonte_total_atendimentos VARCHAR(30) NOT NULL DEFAULT 'zendesk'`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS erro_total_atendimentos TEXT`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_par_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_par_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_gestor_percent NUMERIC(8,2)`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS avaliacoes_pares_gestor_total INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS fonte_avaliacoes_pares VARCHAR(30) NOT NULL DEFAULT 'google_sheets'`);
  await db.query(`ALTER TABLE historico_csat_consultor ADD COLUMN IF NOT EXISTS erro_avaliacoes_pares TEXT`);

  await db.query(`CREATE TABLE IF NOT EXISTS historico_ranking_consultores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consultor_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    mes_ref DATE NOT NULL,
    metrica VARCHAR(40) NOT NULL,
    score NUMERIC(8,2),
    total_registros INTEGER NOT NULL DEFAULT 0,
    positivos INTEGER,
    negativos INTEGER,
    fonte VARCHAR(30) NOT NULL DEFAULT 'database',
    erro TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (consultor_id, mes_ref, metrica)
  )`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_hist_rank_mes_ref ON historico_ranking_consultores(mes_ref)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_hist_rank_consultor ON historico_ranking_consultores(consultor_id)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_hist_rank_metrica ON historico_ranking_consultores(metrica)`);

  await db.query(`CREATE TABLE IF NOT EXISTS historico_reavaliacoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    avaliacao_id UUID REFERENCES avaliacoes(id) ON DELETE SET NULL,
    avaliador_id UUID REFERENCES usuarios(id),
    nome_avaliador VARCHAR(200),
    nota_solucao SMALLINT,
    nota_empatia SMALLINT,
    nota_conhecimento_produto SMALLINT,
    nota_final SMALLINT,
    observacoes TEXT,
    motivo TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Adiciona colunas de nota ao histórico se a tabela já existia
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nome_avaliador VARCHAR(200)`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_solucao SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_empatia SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_conhecimento_produto SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_final SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS observacoes TEXT`);
  
  // Migra dados de nota_conducao para nota_conhecimento_produto se existir (tanto em avaliacoes quanto historico)
  await db.query(`
    DO $$ 
    BEGIN
      -- Migra tabela avaliacoes
      IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='avaliacoes' AND column_name='nota_conducao') THEN
        ALTER TABLE avaliacoes ADD COLUMN IF NOT EXISTS nota_conhecimento_produto SMALLINT CHECK (nota_conhecimento_produto IN (0,25,50,75,100));
        UPDATE avaliacoes SET nota_conhecimento_produto = nota_conducao WHERE nota_conhecimento_produto IS NULL;
        ALTER TABLE avaliacoes DROP COLUMN IF EXISTS nota_conducao;
      END IF;
      -- Migra tabela historico_reavaliacoes
      IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='historico_reavaliacoes' AND column_name='nota_conducao') THEN
        UPDATE historico_reavaliacoes SET nota_conhecimento_produto = nota_conducao WHERE nota_conhecimento_produto IS NULL;
        ALTER TABLE historico_reavaliacoes DROP COLUMN IF EXISTS nota_conducao;
      END IF;
    END $$;
  `);
  
  // Cria tabela de tickets descartados
  await db.query(`CREATE TABLE IF NOT EXISTS tickets_descartados (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    id_zendesk VARCHAR(255) NOT NULL,
    consultor_id UUID REFERENCES usuarios(id) ON DELETE SET NULL,
    nome_consultor VARCHAR(255),
    descartado_por VARCHAR(255) NOT NULL,
    descartado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    motivo TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_tickets_descartados_ticket_id ON tickets_descartados(ticket_id)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_tickets_descartados_id_zendesk ON tickets_descartados(id_zendesk)`);

  await db.query(`CREATE TABLE IF NOT EXISTS configuracoes_responsavel (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    usuario_id UUID NOT NULL UNIQUE REFERENCES usuarios(id) ON DELETE CASCADE,
    zendesk_subdominio VARCHAR(200),
    zendesk_email VARCHAR(200),
    zendesk_token VARCHAR(500),
    smtp_servidor VARCHAR(200),
    smtp_porta INTEGER DEFAULT 587,
    smtp_seguro BOOLEAN DEFAULT FALSE,
    smtp_usuario VARCHAR(200),
    smtp_senha VARCHAR(500),
    smtp_nome_remetente VARCHAR(200) DEFAULT 'TecnoIT',
    ia_chave_api VARCHAR(500),
    ia_modelo VARCHAR(100) DEFAULT 'gemini-2.0-flash',
    documentacao_base TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_cfg_resp_usuario ON configuracoes_responsavel(usuario_id)`);

  await db.query(`CREATE TABLE IF NOT EXISTS configuracoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    zendesk_subdominio VARCHAR(200),
    zendesk_email VARCHAR(200),
    zendesk_token VARCHAR(500),
    smtp_servidor VARCHAR(200),
    smtp_porta INTEGER DEFAULT 587,
    smtp_seguro BOOLEAN DEFAULT FALSE,
    smtp_usuario VARCHAR(200),
    smtp_senha VARCHAR(500),
    smtp_nome_remetente VARCHAR(200) DEFAULT 'TecnoIT',
    ia_chave_api VARCHAR(500),
    ia_modelo VARCHAR(100) DEFAULT 'gemini-2.0-flash',
    documentacao_base TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Garante linha única de configurações
  await db.query(`INSERT INTO configuracoes (id) SELECT gen_random_uuid() WHERE NOT EXISTS (SELECT 1 FROM configuracoes)`);

  // Cria usuário admin se não existir
  const { rows } = await db.query("SELECT id FROM usuarios WHERE login = 'admin' LIMIT 1");
  if (!rows.length) {
    console.log('\n  Criando usuário admin...');
    const ah = await bcrypt.hash('admin123', 10);
    await db.query(`
      INSERT INTO usuarios (login, nome, email, senha_hash, papel)
      VALUES ('admin', 'Administrador', 'admin@empresa.com', $1, 'admin')
      ON CONFLICT (login) DO NOTHING
    `, [ah]);
    console.log('  Criado! Login: admin | Senha: admin123');
    console.log('  Execute POST /api/admin/sync-agents para importar agentes do Zendesk Chat\n');
  }
}

app.listen(PORT, async () => {
  try {
    await autoInit();
    console.log('\n  http://localhost:'+PORT);
    console.log('  Webhook: POST http://localhost:'+PORT+'/webhook/zendesk\n');
  } catch(e) {
    console.error('[INIT ERROR]', e.message);
    console.error('Verifique DATABASE_URL no arquivo .env');
  }
});


