-- ════════════════════════════════════════════════════════════
--  QA Atendimentos — Schema PostgreSQL
--  Rodar uma vez: psql $DATABASE_URL -f migrate.sql
-- ════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- USUÁRIOS
CREATE TABLE IF NOT EXISTS usuarios (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  login         VARCHAR(100) UNIQUE NOT NULL,
  nome          VARCHAR(200) NOT NULL,
  email         VARCHAR(200),
  senha_hash    VARCHAR(200) NOT NULL,
  papel         VARCHAR(20)  NOT NULL DEFAULT 'consultant'
                CHECK (papel IN ('consultant','evaluator','manager','admin')),
  id_zendesk    VARCHAR(50)  UNIQUE,
  criado_em     TIMESTAMPTZ  DEFAULT NOW(),
  atualizado_em TIMESTAMPTZ  DEFAULT NOW()
);

-- GRUPOS
CREATE TABLE IF NOT EXISTS grupos (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  id_zendesk      VARCHAR(50) UNIQUE NOT NULL,
  nome            VARCHAR(200) UNIQUE NOT NULL,
  sincronizado_em TIMESTAMPTZ DEFAULT NOW()
);

-- IDS ZENDESK DO ATENDENTE (assignee_ids)
CREATE TABLE IF NOT EXISTS ids_atendente (
  usuario_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
  id_zendesk VARCHAR(50) NOT NULL,
  PRIMARY KEY (usuario_id, id_zendesk)
);
CREATE INDEX IF NOT EXISTS idx_ids_atendente_zendesk ON ids_atendente(id_zendesk);

-- MEMBROS DE GRUPO
CREATE TABLE IF NOT EXISTS membros_grupo (
  usuario_id UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
  grupo_id   UUID NOT NULL REFERENCES grupos(id)   ON DELETE CASCADE,
  PRIMARY KEY (usuario_id, grupo_id)
);

-- RESPONSÁVEIS POR GRUPO (N:N)
CREATE TABLE IF NOT EXISTS responsaveis_grupo (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  grupo_id     UUID NOT NULL REFERENCES grupos(id)   ON DELETE CASCADE,
  usuario_id   UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
  atribuido_em TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (grupo_id, usuario_id)
);

-- TICKETS (normal + csat unificados)
CREATE TABLE IF NOT EXISTS tickets (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  id_zendesk            VARCHAR(100) UNIQUE NOT NULL,
  tipo                  VARCHAR(10)  NOT NULL DEFAULT 'normal' CHECK (tipo IN ('normal','csat')),
  assunto               VARCHAR(500),
  descricao             TEXT,
  status                VARCHAR(50)  DEFAULT 'resolvido',
  canal                 VARCHAR(50)  DEFAULT 'web',
  consultor_id          UUID REFERENCES usuarios(id),
  id_assignee_zendesk   VARCHAR(50),
  nome_consultor        VARCHAR(200),
  nome_cliente          VARCHAR(200),
  email_cliente         VARCHAR(200),
  tags                  TEXT[]       DEFAULT '{}',
  iniciado_por_ia       BOOLEAN      DEFAULT FALSE,
  avaliado              BOOLEAN      DEFAULT FALSE,
  descartado            BOOLEAN      DEFAULT FALSE,
  em_reavaliacao        BOOLEAN      DEFAULT FALSE,
  nota_csat             VARCHAR(20),
  comentario_csat       TEXT,
  analise_ia            JSONB,
  analise_ia_em         TIMESTAMPTZ,
  descartado_em         TIMESTAMPTZ,
  descartado_por        VARCHAR(200),
  comentario_consultor  TEXT,
  comentario_em         TIMESTAMPTZ,
  resposta_consultor    TEXT,
  resposta_em           TIMESTAMPTZ,
  criado_no_zendesk     TIMESTAMPTZ,
  resolvido_no_zendesk  TIMESTAMPTZ,
  importado_em          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tickets_tipo      ON tickets(tipo);
CREATE INDEX IF NOT EXISTS idx_tickets_assignee  ON tickets(id_assignee_zendesk);
CREATE INDEX IF NOT EXISTS idx_tickets_avaliado  ON tickets(avaliado, descartado);

-- AVALIAÇÕES
CREATE TABLE IF NOT EXISTS avaliacoes (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ticket_id           UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
  avaliador_id        UUID REFERENCES usuarios(id),
  nome_avaliador      VARCHAR(200),
  id_assignee_zendesk VARCHAR(50),
  nome_consultor      VARCHAR(200),
  nota_solucao        SMALLINT CHECK (nota_solucao  IN (0,25,50,75,100)),
  nota_empatia        SMALLINT CHECK (nota_empatia  IN (0,25,50,75,100)),
  nota_conducao       SMALLINT CHECK (nota_conducao IN (0,25,50,75,100)),
  nota_final          SMALLINT,
  observacoes         TEXT,
  origem              VARCHAR(10) DEFAULT 'normal' CHECK (origem IN ('normal','csat')),
  criado_em           TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_avaliacoes_ticket   ON avaliacoes(ticket_id);
CREATE INDEX IF NOT EXISTS idx_avaliacoes_assignee ON avaliacoes(id_assignee_zendesk);

-- HISTÓRICO DE REAVALIAÇÕES
CREATE TABLE IF NOT EXISTS historico_reavaliacoes (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ticket_id      UUID NOT NULL REFERENCES tickets(id)    ON DELETE CASCADE,
  avaliacao_id   UUID          REFERENCES avaliacoes(id) ON DELETE SET NULL,
  avaliador_id   UUID          REFERENCES usuarios(id),
  nome_avaliador VARCHAR(200),
  nota_solucao   SMALLINT,
  nota_empatia   SMALLINT,
  nota_conducao  SMALLINT,
  nota_final     SMALLINT,
  observacoes    TEXT,
  motivo         TEXT,
  criado_em      TIMESTAMPTZ DEFAULT NOW()
);

-- Colunas adicionadas ao histórico (idempotente para bancos já existentes)
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nome_avaliador VARCHAR(200);
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_solucao  SMALLINT;
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_empatia  SMALLINT;
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_conducao SMALLINT;
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS nota_final    SMALLINT;
ALTER TABLE historico_reavaliacoes ADD COLUMN IF NOT EXISTS observacoes   TEXT;

-- CONFIGURAÇÕES (linha única)
CREATE TABLE IF NOT EXISTS configuracoes (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  zendesk_subdominio   VARCHAR(200),
  zendesk_email        VARCHAR(200),
  zendesk_token        VARCHAR(500),
  smtp_servidor        VARCHAR(200),
  smtp_porta           INTEGER     DEFAULT 587,
  smtp_seguro          BOOLEAN     DEFAULT FALSE,
  smtp_usuario         VARCHAR(200),
  smtp_senha           VARCHAR(500),
  smtp_nome_remetente  VARCHAR(200) DEFAULT 'Avaliações de Atendimento',
  ia_chave_api         VARCHAR(500),
  ia_modelo            VARCHAR(100) DEFAULT 'gemini-2.0-flash',
  documentacao_base    TEXT,
  atualizado_em        TIMESTAMPTZ  DEFAULT NOW()
);

INSERT INTO configuracoes (id)
SELECT gen_random_uuid() WHERE NOT EXISTS (SELECT 1 FROM configuracoes);

-- Adiciona coluna email_enviado na tabela avaliacoes (idempotente)
ALTER TABLE avaliacoes ADD COLUMN IF NOT EXISTS email_enviado BOOLEAN NOT NULL DEFAULT FALSE;
