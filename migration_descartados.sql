-- Migration: Tabela para rastrear tickets descartados
-- Executar este SQL no banco de dados PostgreSQL

CREATE TABLE IF NOT EXISTS tickets_descartados (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
  id_zendesk VARCHAR(255) NOT NULL,
  consultor_id UUID REFERENCES usuarios(id) ON DELETE SET NULL,
  nome_consultor VARCHAR(255),
  descartado_por VARCHAR(255) NOT NULL,
  descartado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  motivo TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_tickets_descartados_ticket_id ON tickets_descartados(ticket_id);
CREATE INDEX IF NOT EXISTS idx_tickets_descartados_id_zendesk ON tickets_descartados(id_zendesk);
CREATE INDEX IF NOT EXISTS idx_tickets_descartados_consultor_id ON tickets_descartados(consultor_id);
CREATE INDEX IF NOT EXISTS idx_tickets_descartados_descartado_em ON tickets_descartados(descartado_em DESC);

-- Comentários
COMMENT ON TABLE tickets_descartados IS 'Histórico de tickets descartados com informações do consultor e data/hora';
COMMENT ON COLUMN tickets_descartados.ticket_id IS 'Referência ao ticket descartado';
COMMENT ON COLUMN tickets_descartados.id_zendesk IS 'ID do ticket no Zendesk';
COMMENT ON COLUMN tickets_descartados.consultor_id IS 'ID do consultor responsável pelo ticket';
COMMENT ON COLUMN tickets_descartados.descartado_por IS 'Nome do avaliador que descartou';
COMMENT ON COLUMN tickets_descartados.descartado_em IS 'Data e hora do descarte';
