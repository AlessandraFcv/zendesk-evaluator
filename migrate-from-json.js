/**
 * migrate-from-json.js
 * Migra dados dos arquivos JSON para o banco PostgreSQL.
 * Rodar UMA VEZ após criar as tabelas com migrate.sql.
 *
 *   node migrate-from-json.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const fs   = require('fs');
const path = require('path');

const pool     = new Pool({ connectionString: process.env.DATABASE_URL });
const DATA_DIR = path.join(__dirname, 'data');

function readJSON(file, fallback = []) {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), 'utf8')); }
  catch { return fallback; }
}

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    /* 1. USUÁRIOS */
    const users = readJSON('users.json');
    console.log(`\n[1/6] Migrando ${users.length} usuários...`);
    for (const u of users) {
      await client.query(`
        INSERT INTO usuarios (id, login, nome, email, senha_hash, papel, id_zendesk, criado_em)
        VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT (id) DO UPDATE SET login=EXCLUDED.login, nome=EXCLUDED.nome, email=EXCLUDED.email,
          senha_hash=EXCLUDED.senha_hash, papel=EXCLUDED.papel, id_zendesk=EXCLUDED.id_zendesk
      `, [u.id, u.username, u.name, u.email||null, u.password, u.role||'consultant', u.zendesk_agent_id||null]);
      for (const aid of (u.assignee_ids||[])) {
        await client.query(
          'INSERT INTO ids_atendente (usuario_id, id_zendesk) VALUES ($1,$2) ON CONFLICT DO NOTHING',
          [u.id, String(aid)]
        );
      }
    }
    console.log('   ✅ Usuários migrados');

    /* 2. GRUPOS */
    const groups = readJSON('groups.json');
    console.log(`\n[2/6] Migrando ${groups.length} grupos...`);
    const grupoIdMap = {};
    for (const g of groups) {
      const { rows } = await client.query(`
        INSERT INTO grupos (id_zendesk, nome, sincronizado_em) VALUES ($1,$2,NOW())
        ON CONFLICT (id_zendesk) DO UPDATE SET nome=EXCLUDED.nome RETURNING id
      `, [String(g.id), g.name]);
      grupoIdMap[String(g.id)] = rows[0].id;
    }
    for (const u of users) {
      for (const gName of (u.zendesk_groups||[])) {
        const grp = groups.find(g => g.name === gName);
        if (!grp) continue;
        const guuid = grupoIdMap[String(grp.id)];
        if (!guuid) continue;
        await client.query('INSERT INTO membros_grupo (usuario_id, grupo_id) VALUES ($1,$2) ON CONFLICT DO NOTHING', [u.id, guuid]);
      }
    }
    for (const g of groups) {
      const mids = g.manager_ids || (g.manager_id ? [g.manager_id] : []);
      const guuid = grupoIdMap[String(g.id)];
      if (!guuid) continue;
      for (const mid of mids) {
        await client.query('INSERT INTO responsaveis_grupo (grupo_id, usuario_id) VALUES ($1,$2) ON CONFLICT DO NOTHING', [guuid, mid]);
      }
    }
    console.log('   ✅ Grupos migrados');

    /* 3. CONFIGURAÇÕES */
    const cfg = readJSON('config.json', {});
    console.log('\n[3/6] Migrando configurações...');
    await client.query(`
      UPDATE configuracoes SET
        zendesk_subdominio=$1, zendesk_email=$2, zendesk_token=$3,
        smtp_servidor=$4, smtp_porta=$5, smtp_seguro=$6, smtp_usuario=$7, smtp_senha=$8,
        smtp_nome_remetente=$9, ia_chave_api=$10, ia_modelo=$11, documentacao_base=$12, atualizado_em=NOW()
    `, [cfg.zendeskSubdomain||null, cfg.zendeskEmail||null, cfg.zendeskToken||null,
        cfg.smtpHost||null, cfg.smtpPort||587, cfg.smtpSecure||false, cfg.smtpUser||null, cfg.smtpPass||null,
        cfg.smtpFromName||'Avaliações de Atendimento', cfg.anthropicKey||null,
        cfg.aiModel||'gemini-2.0-flash', cfg.basicampDocs||null]);
    console.log('   ✅ Configurações migradas');

    /* 4. TICKETS NORMAIS */
    const tickets = readJSON('tickets.json');
    console.log(`\n[4/6] Migrando ${tickets.length} tickets normais...`);
    const assigneeToUserId = {};
    for (const u of users) for (const aid of (u.assignee_ids||[])) assigneeToUserId[String(aid)] = u.id;
    for (const t of tickets) {
      const zid = String(t.zendesk_id||t.id);
      await client.query(`
        INSERT INTO tickets (id_zendesk,tipo,assunto,descricao,status,canal,consultor_id,id_assignee_zendesk,
          nome_consultor,nome_cliente,email_cliente,tags,iniciado_por_ia,avaliado,descartado,em_reavaliacao,
          analise_ia,analise_ia_em,descartado_em,descartado_por,comentario_consultor,comentario_em,
          resposta_consultor,resposta_em,criado_no_zendesk,resolvido_no_zendesk,importado_em)
        VALUES ($1,'normal',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
        ON CONFLICT (id_zendesk) DO NOTHING
      `, [zid, t.subject||'Sem assunto', t.description||'', t.status||'resolvido', t.channel||'web',
          assigneeToUserId[String(t.assignee_id)]||null, String(t.assignee_id||''), t.consultant_name||'',
          t.requester_name||'Cliente', t.requester_email||null, t.tags||[], t.ai_initiated||false,
          t.evaluated||false, t.discarded||false, t.reevaluating||false,
          t.ai_review?JSON.stringify(t.ai_review):null, t.ai_review_at||null,
          t.discarded_at||null, t.discarded_by||null,
          t.consultant_comment||null, t.comment_date||null,
          t.consultant_reply||null, t.reply_date||null,
          t.created_at||null, t.solved_at||null, t.received_at||null]);
    }
    console.log('   ✅ Tickets normais migrados');

    /* 5. TICKETS CSAT */
    const csat = readJSON('csat_tickets.json');
    console.log(`\n[5/6] Migrando ${csat.length} tickets CSAT...`);
    for (const t of csat) {
      const zid = String(t.zendesk_id||t.id);
      await client.query(`
        INSERT INTO tickets (id_zendesk,tipo,assunto,status,canal,consultor_id,id_assignee_zendesk,nome_consultor,
          nome_cliente,tags,iniciado_por_ia,avaliado,descartado,nota_csat,comentario_csat,
          analise_ia,analise_ia_em,resolvido_no_zendesk,importado_em)
        VALUES ($1,'csat',$2,'resolvido',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT (id_zendesk) DO NOTHING
      `, [zid, t.subject||'Sem assunto', t.channel||'web',
          assigneeToUserId[String(t.assignee_id)]||null, String(t.assignee_id||''),
          t.consultant_name||'', t.requester_name||'Cliente', t.tags||[], t.ai_initiated||false,
          t.evaluated||false, t.discarded||false,
          t.satisfaction||'bad', t.satisfaction_comment||null,
          t.ai_review?JSON.stringify(t.ai_review):null, t.ai_review_at||null,
          t.solved_at||null, t.received_at||null]);
    }
    console.log('   ✅ Tickets CSAT migrados');

    /* 6. AVALIAÇÕES */
    const evals = readJSON('evaluations.json');
    console.log(`\n[6/6] Migrando ${evals.length} avaliações...`);
    const allT = (await client.query('SELECT id, id_zendesk FROM tickets')).rows;
    const tidMap = {};
    allT.forEach(r => { tidMap[r.id_zendesk] = r.id; });
    for (const e of evals) {
      const tid = tidMap[String(e.zendesk_id)];
      if (!tid) { console.warn(`   ⚠️  ticket ${e.zendesk_id} não encontrado, pulando`); continue; }
      await client.query(`
        INSERT INTO avaliacoes (id,ticket_id,avaliador_id,nome_avaliador,id_assignee_zendesk,nome_consultor,
          nota_solucao,nota_empatia,nota_conducao,nota_final,observacoes,origem,criado_em)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT (id) DO NOTHING
      `, [e.id, tid, e.evaluator_id||null, e.evaluator_name||'', String(e.assignee_id||''),
          e.consultant_name||'', e.scores?.solucao??0, e.scores?.empatia??0, e.scores?.conducao??0,
          e.overall??0, e.notes||'', e.source||'normal', e.created_at||new Date().toISOString()]);
    }
    console.log('   ✅ Avaliações migradas');

    await client.query('COMMIT');
    console.log('\n✅ Migração concluída com sucesso!\n');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('\n❌ Erro:', err.message);
    console.error(err.stack);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
