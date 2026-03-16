require('dotenv').config();
const express    = require('express');
const bcrypt     = require('bcryptjs');
const jwt        = require('jsonwebtoken');
const cors       = require('cors');
const https      = require('https');
const nodemailer = require('nodemailer');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const { v4: uuidv4 } = require('uuid');
const fs   = require('fs');
const path = require('path');
const db   = require('./db');

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

// Retorna assignee_ids Zendesk dos grupos gerenciados por userId
async function getManagerAgentIds(userId) {
  const { rows: grps } = await db.query(`
    SELECT g.id FROM grupos g
    JOIN responsaveis_grupo rg ON rg.grupo_id = g.id
    WHERE rg.usuario_id = $1
  `, [userId]);
  if (!grps.length) return null;
  const grupoIds = grps.map(g => g.id);
  const { rows } = await db.query(`
    SELECT ia.id_zendesk
    FROM ids_atendente ia
    JOIN membros_grupo mg ON mg.usuario_id = ia.usuario_id
    WHERE mg.grupo_id = ANY($1)
  `, [grupoIds]);
  return rows.map(r => r.id_zendesk);
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

async function canManage(req, res, next) {
  if (req.user.role === 'admin' || req.user.role === 'evaluator' || req.user.role === 'manager') return next();
  return res.status(403).json({ error: 'Acesso restrito' });
}

/* ══════════════════════════════════════════════════════════════
   ZENDESK + IA
══════════════════════════════════════════════════════════════ */

function zendeskRequest(urlPath, cfg) {
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(cfg.zendesk_email + '/token:' + cfg.zendesk_token).toString('base64');
    const url  = 'https://' + cfg.zendesk_subdominio + '.zendesk.com' + urlPath;
    const req  = https.request(url, { method:'GET', headers:{ 'Authorization':'Basic '+auth, 'Content-Type':'application/json' } }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try {
          const p = JSON.parse(d);
          if (res.statusCode !== 200) return reject(new Error(p.error || p.description || 'HTTP ' + res.statusCode));
          resolve(p);
        } catch (e) { reject(new Error('Resposta invalida do Zendesk (HTTP '+res.statusCode+'): ' + d.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function zendeskPut(urlPath, body, cfg) {
  return new Promise((resolve, reject) => {
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
        try {
          const p = JSON.parse(d);
          if (res.statusCode < 200 || res.statusCode > 299) return reject(new Error(p.error || p.description || 'HTTP ' + res.statusCode));
          resolve(p);
        } catch (e) { reject(new Error('Resposta invalida do Zendesk PUT: ' + d.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
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
  await t.sendMail({ from:'"'+(cfg.smtp_nome_remetente||'Avaliações de Atendimento')+'" <'+cfg.smtp_usuario+'>', ...opts });
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

/* ══════════════════════════════════════════════════════════════
   AUTH
══════════════════════════════════════════════════════════════ */

app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Usuario e senha obrigatorios' });
  try {
    const { rows } = await db.query(
      'SELECT * FROM usuarios WHERE LOWER(login) = LOWER($1) LIMIT 1',
      [username]
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
    res.json({ token, user:{ id:user.id, name:user.nome, username:user.login, role:user.papel, email:user.email } });
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
   CONFIG
══════════════════════════════════════════════════════════════ */

app.get('/api/admin/config', authenticate, adminOnly, async (req, res) => {
  try {
    const c = await getCfg();
    res.json({
      zendeskSubdomain: c.zendesk_subdominio||'', zendeskEmail: c.zendesk_email||'', zendeskHasToken: !!c.zendesk_token,
      smtpHost: c.smtp_servidor||'', smtpPort: c.smtp_porta||587, smtpSecure: c.smtp_seguro||false,
      smtpUser: c.smtp_usuario||'', smtpHasPass: !!c.smtp_senha, smtpFromName: c.smtp_nome_remetente||'Avaliações de Atendimento',
      anthropicHasKey: !!c.ia_chave_api, aiModel: c.ia_modelo||'gemini-2.0-flash', basicampDocs: c.documentacao_base||''
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/config', authenticate, adminOnly, async (req, res) => {
  const b = req.body;
  try {
    const c = await getCfg();
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

app.get('/api/admin/users', authenticate, adminOnly, async (req, res) => {
  try {
    const { rows } = await db.query(`
      SELECT u.id, u.login AS username, u.nome AS name, u.email, u.papel AS role,
             u.id_zendesk AS zendesk_agent_id,
             COALESCE(array_agg(DISTINCT ia.id_zendesk) FILTER (WHERE ia.id_zendesk IS NOT NULL), '{}') AS assignee_ids,
             COALESCE(array_agg(DISTINCT g.nome)        FILTER (WHERE g.nome IS NOT NULL),        '{}') AS zendesk_groups
      FROM usuarios u
      LEFT JOIN ids_atendente ia ON ia.usuario_id = u.id
      LEFT JOIN membros_grupo mg ON mg.usuario_id = u.id
      LEFT JOIN grupos g         ON g.id = mg.grupo_id
      GROUP BY u.id
      ORDER BY u.nome
    `);
    res.json({ users: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users/:id/email', authenticate, adminOnly, async (req, res) => {
  try {
    await db.query('UPDATE usuarios SET email = $1, atualizado_em = NOW() WHERE id = $2', [req.body.email || '', req.params.id]);
    res.json({ message: 'E-mail atualizado' });
  } catch(e) { res.status(500).json({ error: e.message }); }
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

/* ══════════════════════════════════════════════════════════════
   SYNC AGENTS FROM ZENDESK
══════════════════════════════════════════════════════════════ */

app.post('/api/admin/sync-agents', authenticate, adminOnly, async (req, res) => {
  const cfg = await getCfg();
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  try {
    // 1. Busca todos os grupos
    const groupsData = await zendeskRequest('/api/v2/groups.json?per_page=100', cfg).catch(() => ({ groups: [] }));
    const groupMap = {}; // zendesk_group_id → name
    for (const g of (groupsData.groups || [])) {
      groupMap[String(g.id)] = g.name;
      await db.query(`
        INSERT INTO grupos (id_zendesk, nome, sincronizado_em)
        VALUES ($1, $2, NOW())
        ON CONFLICT (id_zendesk) DO UPDATE SET nome = EXCLUDED.nome, sincronizado_em = NOW()
      `, [String(g.id), g.name]);
    }

    // 2. Busca TODOS os membros de grupo de uma vez (paginando)
    //    Retorna: [{ user_id, group_id }, ...]
    const membershipMap = {}; // zendesk_user_id → [zendesk_group_id, ...]
    let gmPage = 1, gmHasMore = true;
    while (gmHasMore) {
      const gmData = await zendeskRequest(`/api/v2/group_memberships.json?per_page=100&page=${gmPage}`, cfg)
        .catch(() => ({ group_memberships: [], next_page: null }));
      for (const m of (gmData.group_memberships || [])) {
        const uid = String(m.user_id);
        const gid = String(m.group_id);
        if (!membershipMap[uid]) membershipMap[uid] = [];
        if (!membershipMap[uid].includes(gid)) membershipMap[uid].push(gid);
      }
      gmHasMore = !!(gmData.next_page);
      gmPage++;
      if (gmPage > 50) break;
    }

    // 3. Busca todos os agentes paginando
    let allAgents = [], page = 1, hasMore = true;
    while (hasMore) {
      const data = await zendeskRequest(`/api/v2/users.json?role=agent&per_page=100&page=${page}`, cfg);
      allAgents = allAgents.concat(data.users || []);
      hasMore = !!(data.next_page);
      page++;
      if (page > 20) break;
    }

    const dh = await bcrypt.hash('senha123', 10);
    const normName  = s => (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim();
    const normEmail = s => (s||'').toLowerCase().trim();

    const resolveGroups = (agentId) => {
      const gids = membershipMap[String(agentId)] || [];
      return gids.map(gid => groupMap[gid]).filter(Boolean);
    };

    let created = 0, updated = 0, linked = 0;

    for (const agent of allAgents) {
      if (!agent.active) continue;
      const agentId    = String(agent.id);
      const groups     = resolveGroups(agentId);
      const username   = normName(agent.name).replace(/\s+/g,'.');
      const agentEmail = normEmail(agent.email || '');

      // Tenta encontrar usuário existente
      let { rows } = await db.query(`
        SELECT u.id, u.id_zendesk FROM usuarios u
        WHERE u.id_zendesk = $1
           OR LOWER(u.nome) = LOWER($2)
           OR (u.email <> '' AND LOWER(u.email) = $3)
        LIMIT 1
      `, [agentId, agent.name, agentEmail]);

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
          UPDATE usuarios SET nome = $1, id_zendesk = $2::varchar,
            email = CASE WHEN (email IS NULL OR email = '') THEN $3::varchar ELSE email END,
            atualizado_em = NOW()
          WHERE id = $4
        `, [agent.name, agentId, agentEmail, userId]);
        wasLinked ? linked++ : updated++;
      } else {
        const ins = await db.query(`
          INSERT INTO usuarios (login, nome, email, senha_hash, papel, id_zendesk)
          VALUES ($1, $2, $3, $4, 'consultant', $5)
          ON CONFLICT (login) DO UPDATE SET nome = EXCLUDED.nome RETURNING id
        `, [username, agent.name, agentEmail, dh, agentId]);
        userId = ins.rows[0].id;
        created++;
      }

      // Garante assignee_id na tabela ids_atendente
      await db.query(`
        INSERT INTO ids_atendente (usuario_id, id_zendesk) VALUES ($1, $2) ON CONFLICT DO NOTHING
      `, [userId, agentId]);

      // Atualiza membros_grupo — limpa e reinsere para refletir estado atual
      await db.query('DELETE FROM membros_grupo WHERE usuario_id = $1', [userId]);
      for (const gName of groups) {
        const gr = await db.query('SELECT id FROM grupos WHERE nome = $1 LIMIT 1', [gName]);
        if (!gr.rows.length) continue;
        await db.query(`
          INSERT INTO membros_grupo (usuario_id, grupo_id) VALUES ($1, $2) ON CONFLICT DO NOTHING
        `, [userId, gr.rows[0].id]);
      }
    }

    const parts = [];
    if (created) parts.push(`${created} criado(s)`);
    if (linked)  parts.push(`${linked} vinculado(s)`);
    if (updated) parts.push(`${updated} atualizado(s)`);
    res.json({ message: parts.join(', ') || 'Nenhuma alteração', created, linked, updated, total: allAgents.length });
  } catch(e) { res.status(500).json({ error: 'Erro ao sincronizar: '+e.message }); }
});

/* ══════════════════════════════════════════════════════════════
   GRUPOS
══════════════════════════════════════════════════════════════ */

app.get('/api/admin/groups', authenticate, canEvaluate, async (req, res) => {
  try {
    const { rows } = await db.query(`
      SELECT g.id, g.nome AS name,
        COALESCE(
          json_agg(DISTINCT jsonb_build_object(
            'id',         u_cons.id,
            'name',       u_cons.nome,
            'id_zendesk', u_cons.id_zendesk
          )) FILTER (WHERE u_cons.id IS NOT NULL),
        '[]') AS consultants,
        COALESCE(array_agg(DISTINCT rg.usuario_id) FILTER (WHERE rg.usuario_id IS NOT NULL), '{}') AS manager_ids
      FROM grupos g
      LEFT JOIN membros_grupo mg ON mg.grupo_id = g.id
      LEFT JOIN usuarios u_cons  ON u_cons.id = mg.usuario_id AND u_cons.id_zendesk IS NOT NULL
      LEFT JOIN responsaveis_grupo rg ON rg.grupo_id = g.id
      GROUP BY g.id, g.nome
      HAVING COUNT(DISTINCT u_cons.id) > 0
      ORDER BY g.nome
    `);
    res.json({ groups: rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
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

/* ══════════════════════════════════════════════════════════════
   MANAGER: MEU GRUPO
══════════════════════════════════════════════════════════════ */

app.get('/api/manager/group', authenticate, async (req, res) => {
  try {
    const { rows: myGroups } = await db.query(`
      SELECT g.id, g.nome AS name
      FROM grupos g JOIN responsaveis_grupo rg ON rg.grupo_id = g.id
      WHERE rg.usuario_id = $1
    `, [req.user.id]);
    if (!myGroups.length) return res.status(403).json({ error: 'Você não é responsável por nenhum grupo' });

    const grupoIds = myGroups.map(g => g.id);

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
  const cfg = await getCfg();
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { assignee_id, year, month, ticket_ids } = req.query;

  try {
    const amap = await getAssigneeMap();
    const { rows: existingRows } = await db.query('SELECT id_zendesk FROM tickets');
    const existing = new Set(existingRows.map(r => r.id_zendesk));

    const mapTicket = (t, aid) => ({
      id: String(t.id), zendesk_id: String(t.id),
      subject: t.subject || 'Sem assunto',
      requester_name: t.via?.source?.from?.name || t.requester?.name || 'Cliente',
      assignee_id: String(aid || t.assignee_id || ''),
      consultant_name: amap[String(aid || t.assignee_id)] || 'ID:'+(aid || t.assignee_id),
      solved_at: t.updated_at,
      tags: t.tags || [],
      ai_initiated: (t.tags || []).includes('claudia_escalado_n2'),
      already_imported: existing.has(String(t.id))
    });

    // Modo 1: busca por números de ticket específicos
    if (ticket_ids) {
      const ids = String(ticket_ids).split(',').map(s => s.trim()).filter(Boolean);
      if (!ids.length) return res.status(400).json({ error: 'Nenhum ID informado' });
      // show_many retorna até 100 tickets por chamada
      const data = await zendeskRequest(`/api/v2/tickets/show_many.json?ids=${ids.join(',')}`, cfg);
      const tickets = (data.tickets || []).map(t => mapTicket(t, null));
      return res.json({ tickets, total: tickets.length });
    }

    // Modo 2: busca por consultor + mês
    if (!assignee_id || !year || !month) return res.status(400).json({ error: 'assignee_id, year e month são obrigatórios' });
    const y = Number(year), m = Number(month);
    const start = new Date(y, m-1, 1).toISOString().slice(0,10);
    const end   = new Date(y, m, 1).toISOString().slice(0,10);
    const q    = encodeURIComponent(`type:ticket assignee:${assignee_id} status:solved solved>=${start} solved<${end}`);
    const data = await zendeskRequest('/api/v2/search.json?query='+q+'&per_page=100&sort_by=solved_at&sort_order=desc', cfg);
    const tickets = (data.results || []).map(t => mapTicket(t, assignee_id));
    res.json({ tickets, total: tickets.length });
  } catch (e) { res.status(500).json({ error: 'Erro Zendesk: '+e.message }); }
});

app.post('/api/zendesk/import', authenticate, canEvaluate, async (req, res) => {
  const cfg = await getCfg();
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
        conducao:  ev.nota_conducao,
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
  const cfg = await getCfg();
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
    const cfg = await getCfg();
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

    const docs   = cfg.documentacao_base ? '\n\nDOCUMENTACAO DE REFERENCIA:\n'+cfg.documentacao_base : '';
    const system = `Voce e um especialista em qualidade de atendimento ao cliente.
Analise a conversa e avalie em 3 criterios (escala 0,25,50,75,100):
- solucao: O agente resolveu o problema do cliente?
- empatia: O agente demonstrou empatia e compreensao?
- conducao: O agente conduziu o atendimento com profissionalismo?

Responda APENAS em JSON valido sem markdown:
{"resumo":"...","pontos_positivos":["..."],"pontos_melhoria":["..."],"scores":{"solucao":75,"empatia":75,"conducao":75},"nota_sugerida":75,"justificativa":"..."}${docs}`;

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
             nota_solucao, nota_empatia, nota_conducao, nota_final, observacoes, motivo)
          VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, 'Reavaliação solicitada')
        `, [
          ticket.id, prev.id, prev.avaliador_id, prev.nome_avaliador,
          prev.nota_solucao, prev.nota_empatia, prev.nota_conducao,
          prev.nota_final, prev.observacoes || ''
        ]);
      }
      await db.query('DELETE FROM avaliacoes WHERE ticket_id = $1', [ticket.id]);
    }

    const { rows: evRows } = await db.query(`
      INSERT INTO avaliacoes (ticket_id, avaliador_id, nome_avaliador, id_assignee_zendesk, nome_consultor,
        nota_solucao, nota_empatia, nota_conducao, nota_final, observacoes, origem, email_enviado)
      VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,'normal',false)
      RETURNING *
    `, [
      ticket.id, req.user.id, req.user.name,
      ticket.id_assignee_zendesk, ticket.nome_consultor,
      Number(toDb(scores.solucao)), Number(toDb(scores.empatia)), Number(toDb(scores.conducao)),
      Number(overall), notes||''
    ]);
    const ev = evRows[0];

    await db.query('UPDATE tickets SET avaliado = true, em_reavaliacao = false WHERE id = $1', [ticket.id]);

    // Tag Zendesk
    try {
      const cfg = await getCfg();
      if (cfg.zendesk_subdominio && cfg.zendesk_token) {
        await zendeskPut(`/api/v2/tickets/${ticket.id_zendesk}.json`, { ticket: { additional_tags: ['ticket_avaliado'] } }, cfg);
      }
    } catch (tagErr) { console.warn('[ZENDESK TAG]', tagErr.message); }

    // E-mail para consultor
    if (sendEmail) {
      try {
        const cfg = await getCfg();
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
    <div style="color:#fff;font-size:1.3rem;font-weight:800;margin-bottom:4px">tecnospeed Avaliações</div>
    <div style="color:rgba(255,255,255,.55);font-size:.7rem;letter-spacing:.1em;text-transform:uppercase">Plataforma de Qualidade · Atendimentos</div>
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
      <tbody>${scoreRow('Solução',ev.nota_solucao,'#0d0f18')}${scoreRow('Empatia',ev.nota_empatia,'#111627')}${scoreRow('Condução',ev.nota_conducao,'#0d0f18')}</tbody>
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
      const cfg = await getCfg();
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
    <div style="color:rgba(255,255,255,.55);font-size:.7rem;letter-spacing:.1em;text-transform:uppercase">Avaliações de Atendimentos</div>
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
  const cfg = await getCfg();
  if (!cfg.zendesk_subdominio || !cfg.zendesk_token) return res.status(400).json({ error: 'Zendesk nao configurado' });
  const { year, month } = req.query;
  if (!year || !month) return res.status(400).json({ error: 'Informe ano e mes' });
  const y = Number(year), m = Number(month);
  const start = new Date(y, m-1, 1).toISOString().slice(0,10);
  const end   = new Date(y, m, 1).toISOString().slice(0,10);
  try {
    const q    = encodeURIComponent('type:ticket satisfaction:bad updated>'+start+' updated<'+end);
    const data = await zendeskRequest('/api/v2/search.json?query='+q+'&per_page=100', cfg);
    const amap = await getAssigneeMap();
    const { rows: existingRows } = await db.query("SELECT id_zendesk FROM tickets WHERE tipo = 'csat'");
    const existing = new Set(existingRows.map(r => r.id_zendesk));
    const tickets = (data.results||[]).map(t => ({
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
  const cfg = await getCfg();
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

app.get('/api/csat/tickets', authenticate, canEvaluate, async (req, res) => {
  const { filter, page=1, limit=15, search } = req.query;
  try {
    const conditions = ["tipo = 'csat'"];
    const params = [];
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

app.get('/api/csat/tickets/:id', authenticate, canEvaluate, async (req, res) => {
  try {
    const { rows } = await db.query("SELECT * FROM tickets WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const t = rows[0];
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
        conducao: ev.nota_conducao,
      }
    } : null;
    res.json({ ticket, evaluation });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/csat/tickets/:id/evaluate', authenticate, canEvaluate, async (req, res) => {
  const { scores, notes, overall } = req.body;
  if (!scores || overall === undefined) return res.status(400).json({ error: 'Dados incompletos' });
  const PTS2 = {1:0, 2:25, 3:50, 4:75, 5:100};
  const toDb2 = v => { const n = Number(v); return PTS2[n] !== undefined ? PTS2[n] : n; };
  try {
    const { rows } = await db.query("SELECT * FROM tickets WHERE id = $1 AND tipo = 'csat'", [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Ticket nao encontrado' });
    const ticket = rows[0];
    if (ticket.avaliado && !ticket.em_reavaliacao) return res.status(400).json({ error: 'Ticket ja avaliado' });
    await db.query(`
      INSERT INTO avaliacoes (ticket_id,avaliador_id,nome_avaliador,id_assignee_zendesk,nome_consultor,nota_solucao,nota_empatia,nota_conducao,nota_final,observacoes,origem,email_enviado)
      VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,'csat',false)
    `, [ticket.id,req.user.id,req.user.name,ticket.id_assignee_zendesk,ticket.nome_consultor,Number(toDb2(scores.solucao)),Number(toDb2(scores.empatia)),Number(toDb2(scores.conducao)),Number(overall),notes||'']);
    await db.query('UPDATE tickets SET avaliado = true, em_reavaliacao = false WHERE id = $1', [ticket.id]);
    // Tags Zendesk
    try {
      const cfg = await getCfg();
      if (cfg.zendesk_subdominio && cfg.zendesk_token) {
        await zendeskPut(`/api/v2/tickets/${ticket.id_zendesk}.json`, { ticket: { additional_tags: ['ticket_avaliado','csat_negativo_avaliado'] } }, cfg);
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
    const cfg = await getCfg();
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
    const docs   = cfg.documentacao_base ? '\n\nDOCUMENTACAO DE REFERENCIA:\n'+cfg.documentacao_base : '';
    const system = 'Voce e um especialista em qualidade de atendimento.\nEste ticket recebeu uma AVALIACAO NEGATIVA do cliente (CSAT ruim). Analise o motivo da insatisfacao e avalie em 3 criterios (0,25,50,75,100): solucao, empatia, conducao.\nResponda APENAS em JSON valido sem markdown:\n{"resumo":"...","pontos_positivos":["..."],"pontos_melhoria":["..."],"motivo_insatisfacao":"...","scores":{"solucao":50,"empatia":50,"conducao":50},"nota_sugerida":50,"justificativa":"..."}'+docs;
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
    let q = 'SELECT * FROM avaliacoes';
    const params = [];
    if (req.user.role === 'consultant') {
      q += ' WHERE id_assignee_zendesk = ANY($1)';
      params.push(req.user.assignee_ids);
    }
    q += ' ORDER BY criado_em DESC';
    const { rows } = await db.query(q, params);
    res.json({ evaluations: rows });
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
        ROUND(AVG(nota_conducao)) AS avg_conducao,
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
        conducao: Number(evalStats.avg_conducao || 0),
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
    senha_hash VARCHAR(200) NOT NULL,
    papel VARCHAR(20) NOT NULL DEFAULT 'consultant' CHECK (papel IN ('consultant','evaluator','manager','admin')),
    id_zendesk VARCHAR(50) UNIQUE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

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

  await db.query(`CREATE TABLE IF NOT EXISTS avaliacoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    avaliador_id UUID REFERENCES usuarios(id),
    nome_avaliador VARCHAR(200),
    id_assignee_zendesk VARCHAR(50),
    nome_consultor VARCHAR(200),
    nota_solucao SMALLINT CHECK (nota_solucao IN (0,25,50,75,100)),
    nota_empatia SMALLINT CHECK (nota_empatia IN (0,25,50,75,100)),
    nota_conducao SMALLINT CHECK (nota_conducao IN (0,25,50,75,100)),
    nota_final SMALLINT,
    observacoes TEXT,
    origem VARCHAR(10) DEFAULT 'normal' CHECK (origem IN ('normal','csat')),
    email_enviado BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Adiciona coluna email_enviado se a tabela já existia sem ela
  await db.query(`ALTER TABLE avaliacoes ADD COLUMN IF NOT EXISTS email_enviado BOOLEAN NOT NULL DEFAULT FALSE`);;

  await db.query(`CREATE INDEX IF NOT EXISTS idx_avaliacoes_ticket ON avaliacoes(ticket_id)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_avaliacoes_assignee ON avaliacoes(id_assignee_zendesk)`);

  await db.query(`CREATE TABLE IF NOT EXISTS historico_reavaliacoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    avaliacao_id UUID REFERENCES avaliacoes(id) ON DELETE SET NULL,
    avaliador_id UUID REFERENCES usuarios(id),
    nome_avaliador VARCHAR(200),
    nota_solucao SMALLINT,
    nota_empatia SMALLINT,
    nota_conducao SMALLINT,
    nota_final SMALLINT,
    observacoes TEXT,
    motivo TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Adiciona colunas de nota ao histórico se a tabela já existia
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nome_avaliador VARCHAR(200)`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_solucao SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_empatia SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_conducao SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_final SMALLINT`);
  await db.query(`ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS observacoes TEXT`);

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
    smtp_nome_remetente VARCHAR(200) DEFAULT 'Avaliações de Atendimento',
    ia_chave_api VARCHAR(500),
    ia_modelo VARCHAR(100) DEFAULT 'gemini-2.0-flash',
    documentacao_base TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
  )`);

  // Garante linha única de configurações
  await db.query(`INSERT INTO configuracoes (id) SELECT gen_random_uuid() WHERE NOT EXISTS (SELECT 1 FROM configuracoes)`);

  // Cria usuários padrão se não existirem
  const { rows } = await db.query("SELECT id FROM usuarios WHERE login = 'admin' LIMIT 1");
  if (!rows.length) {
    console.log('\n  Criando usuários iniciais...');
    const ah = await bcrypt.hash('admin123', 10);
    const dh = await bcrypt.hash('senha123', 10);
    const defaultUsers = [
      ['admin',       'Administrador',  'admin@empresa.com',                    'admin',     ah, null],
      ['tiago',       'Tiago',          'tiago.valverde@tecnospeed.com.br',      'evaluator', dh, null],
      ['leticia',     'Leticia',        'leticia.pessoa@tecnospeed.com.br',      'consultant',dh, '30235984721815'],
      ['erika',       'Erika',          'erika.silva@tecnospeed.com.br',         'consultant',dh, '34323495409943'],
      ['gabriel',     'Gabriel',        'gabriel.carvalho@tecnospeed.com.br',    'consultant',dh, '27566549990295'],
      ['marcelo',     'Marcelo',        'marcelo.rocha@tecnospeed.com.br',       'consultant',dh, '1529827574762'],
      ['henrique',    'Henrique',       'henrique.medeiros@tecnospeed.com.br',   'consultant',dh, '27566497005975'],
      ['alessandra',  'Alessandra',     'alessandra.santos@tecnospeed.com.br',   'consultant',dh, '412356865654'],
      ['emilly',      'Emilly',         'emilly.santana@tecnospeed.com.br',      'consultant',dh, '23339270934935'],
      ['gilberto',    'Gilberto',       'gilberto.almeida@tecnospeed.com.br',    'consultant',dh, '424200531334'],
      ['ives',        'Ives',           'ives.hirose@tecnospeed.com.br',         'consultant',dh, '34323518871063'],
      ['miriam',      'Miriam',         'miriam.alves@tecnospeed.com.br',        'consultant',dh, '34323447873559'],
    ];
    for (const [login, nome, email, papel, hash, zendesk_id] of defaultUsers) {
      const ins = await db.query(`
        INSERT INTO usuarios (login, nome, email, senha_hash, papel, id_zendesk)
        VALUES ($1,$2,$3,$4,$5,$6::varchar)
        ON CONFLICT (login) DO NOTHING
        RETURNING id
      `, [login, nome, email, hash, papel, zendesk_id]);
      if (ins.rows[0] && zendesk_id) {
        await db.query(
          'INSERT INTO ids_atendente (usuario_id, id_zendesk) VALUES ($1,$2) ON CONFLICT DO NOTHING',
          [ins.rows[0].id, zendesk_id]
        );
      }
    }
    console.log('  Criados! admin=admin123 | demais=senha123\n');
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
