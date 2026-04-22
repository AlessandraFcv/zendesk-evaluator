# 🚀 Zendesk Evaluator - Modificações Implementadas

## 📋 Resumo das Funcionalidades Adicionadas

### ✅ 1. Botão para Ver Critérios de Avaliação da IA
- **Localização**: Aparece DEPOIS que a IA faz a análise (dentro da caixa de análise)
- **Funcionalidade**: Botão "📋 Ver Critérios de Avaliação da IA" que busca e exibe o conteúdo da **Documentação Base** configurada em Configurações → Documentação Base
- **Conteúdo**: Mostra exatamente o que foi configurado como critérios/documentação no sistema
- **Quando aparece**: Apenas após a IA concluir a análise (não aparece no preview inicial)
- **Suporta formatação**: Quebras de linha e negrito (**texto**)

### ✅ 2. "Condução" → "Conhecimento do Produto"
- **Modificado em**: Todos os arquivos (backend, frontend, banco de dados, emails)
- **Detalhes**:
  - Campo `nota_conducao` renomeado para `nota_conhecimento_produto`
  - Labels atualizados em todas as telas
  - Emails de avaliação atualizados
  - Export CSV atualizado
  - Migration automática de dados existentes

### ✅ 3. Filtro por Grupos Funcional
- **Localização**: Modal de importação de tickets
- **Funcionalidade**: 
  - Ao selecionar um ou mais grupos, apenas tickets desses grupos são listados
  - Se nenhum consultor específico for selecionado, busca tickets de TODOS os consultores dos grupos selecionados
  - Se um consultor específico for selecionado, busca apenas dele
- **Backend**: Rota `/api/zendesk/import` modificada para aceitar múltiplos `assignee_ids`

### ✅ 4. Sistema de Descarte com Banco de Dados
- **Tabela nova**: `tickets_descartados`
- **Campos salvos**:
  - `ticket_id` - Referência ao ticket descartado
  - `id_zendesk` - ID do ticket no Zendesk
  - `consultor_id` - ID do consultor responsável
  - `nome_consultor` - Nome do consultor
  - `descartado_por` - Nome do avaliador que descartou
  - `descartado_em` - Data e hora do descarte
  - `motivo` - Motivo opcional do descarte
- **Funcionalidade**: Tickets descartados NÃO aparecem mais na lista de importação (nem mesmo se buscar novamente)

### ✅ 5. Preview do Atendimento Antes da Análise IA
- **Fluxo novo**:
  1. Ao clicar em "Avaliar", a conversa completa é carregada AUTOMATICAMENTE e expandida
  2. Avaliador lê a conversa na íntegra
  3. Clica no botão "✨ Iniciar Análise com IA" quando estiver pronto
  4. Apenas então a IA faz a análise
- **Benefício**: Avaliador pode formar opinião própria antes da IA influenciar

### ✅ 6. IA em Primeira Pessoa
- **Modificação**: Prompt da IA alterado para gerar análises em primeira pessoa
- **Exemplo**: 
  - ❌ Antes: "O agente demonstrou empatia..."
  - ✅ Agora: "Eu observei que o agente demonstrou empatia..."
- **Aplicado em**: Análises de tickets normais e CSAT

---

## 📦 Instalação

### 1️⃣ Backup do Banco de Dados
**IMPORTANTE**: Faça backup antes de aplicar as modificações!

```bash
# PostgreSQL backup
pg_dump -U seu_usuario -d zendesk_evaluator > backup_$(date +%Y%m%d_%H%M%S).sql
```

### 2️⃣ Aplicar Migration do Banco
Execute o arquivo `migration_descartados.sql`:

```bash
psql -U seu_usuario -d zendesk_evaluator -f migration_descartados.sql
```

**O que a migration faz:**
- Cria tabela `tickets_descartados` com índices
- Renomeia `nota_conducao` → `nota_conhecimento_produto` (com migração de dados)
- Aplica alterações em `avaliacoes` e `historico_reavaliacoes`

### 3️⃣ Substituir Arquivos

```bash
# Fazer backup dos arquivos atuais
cp server.js server.js.backup
cp public/index.html public/index.html.backup

# Copiar os novos arquivos
cp server.js /caminho/do/seu/projeto/
cp index.html /caminho/do/seu/projeto/public/
```

### 4️⃣ Reiniciar o Servidor

```bash
# Parar o servidor atual
# Ctrl+C ou pkill -f "node server.js"

# Reiniciar
node server.js
```

### 5️⃣ Testar as Funcionalidades

1. ✅ Abra a aplicação
2. ✅ Vá em "Avaliar" → verifique que a conversa abre automaticamente
3. ✅ Clique em "Ver Critérios de Avaliação da IA"
4. ✅ Clique em "Iniciar Análise com IA"
5. ✅ Verifique que o critério aparece como "Conhecimento do Produto"
6. ✅ Descarte um ticket → verifique que ele não aparece mais na importação
7. ✅ Teste filtro por grupos na importação

---

## 🔧 Alterações Técnicas Detalhadas

### Configuração da Documentação Base

Para que o botão "Ver Critérios de Avaliação da IA" funcione, você precisa configurar a **Documentação Base**:

1. **Acesse**: Configurações (ícone de engrenagem) → seção "Configurações de IA"
2. **Campo**: "Documentação Base / Critérios de Avaliação"
3. **Preencha**: Os critérios que a IA deve seguir. Exemplo:

```
**Critérios de Avaliação**

1. **Solução (0-100 pts)**
   - O problema foi completamente resolvido?
   - As informações fornecidas foram corretas e úteis?
   - Houve necessidade de follow-up?

2. **Empatia (0-100 pts)**
   - O tom foi acolhedor e respeitoso?
   - O agente demonstrou compreender a situação do cliente?
   - Houve personalização no atendimento?

3. **Conhecimento do Produto (0-100 pts)**
   - O agente conhecia bem as funcionalidades?
   - As orientações foram claras e precisas?
   - Demonstrou segurança nas respostas?
```

**Dica**: Use `**texto**` para negrito e quebras de linha serão preservadas.

---

## 🔧 Alterações Técnicas Detalhadas

### Backend (server.js)

#### Rotas Novas:
- `GET /api/evaluation-criteria` - Retorna os critérios de avaliação (documentacao_base) para avaliadores verem

#### Rotas Modificadas:
- `GET /api/zendesk/import` - Aceita `assignee_ids` (múltiplos IDs separados por vírgula)
- `POST /api/tickets/:id/discard` - Salva em `tickets_descartados` antes de marcar como descartado
- `POST /api/tickets/:id/ai-review` - Prompt modificado para primeira pessoa
- `POST /api/csat/tickets/:id/ai-review` - Prompt modificado para primeira pessoa

#### Banco de Dados:
- Nova tabela: `tickets_descartados`
- Renomeação de colunas em `avaliacoes` e `historico_reavaliacoes`
- Migration automática de dados existentes

### Frontend (index.html)

#### Funções Novas:
- `showCriteriaModal()` - Modal com critérios detalhados da IA
- `startAIAnalysis(ticketId)` - Inicia análise IA sob demanda
- `loadConversation(ticket)` - Carrega conversa automaticamente expandida

#### Funções Modificadas:
- `openEvalModal()` - Agora mostra preview primeiro, botão de análise depois
- `fetchImportTickets()` - Suporta múltiplos assignee_ids dos grupos selecionados
- Todas referências a "conducao" → "conhecimento_produto"

---

## 🐛 Troubleshooting

### Erro: "relation tickets_descartados does not exist"
**Solução**: Execute o arquivo `migration_descartados.sql`

### Erro: "column nota_conducao does not exist"
**Solução**: A migration deve ter falhado. Execute manualmente:
```sql
ALTER TABLE avaliacoes RENAME COLUMN nota_conducao TO nota_conhecimento_produto;
ALTER TABLE historico_reavaliacoes RENAME COLUMN nota_conducao TO nota_conhecimento_produto;
```

### Tickets descartados ainda aparecem na importação
**Solução**: Verifique se a migration criou a tabela `tickets_descartados` corretamente:
```sql
SELECT * FROM tickets_descartados LIMIT 5;
```

### IA não está respondendo em primeira pessoa
**Solução**: Reinicie o servidor para garantir que o novo código do server.js foi carregado.

### Botão "Ver Critérios" mostra mensagem "Nenhum critério configurado"
**Solução**: Configure a Documentação Base em Configurações → seção "Configurações de IA" → campo "Documentação Base / Critérios de Avaliação"

---

## 📊 Compatibilidade

- ✅ PostgreSQL 12+
- ✅ Node.js 14+
- ✅ Navegadores modernos (Chrome, Firefox, Safari, Edge)

---

## 📞 Suporte

Se encontrar problemas durante a instalação ou uso:

1. Verifique os logs do servidor: `tail -f logs/server.log`
2. Verifique os logs do navegador (F12 → Console)
3. Revise cada passo da instalação
4. Faça rollback se necessário:
   ```bash
   # Restaurar arquivos
   cp server.js.backup server.js
   cp public/index.html.backup public/index.html
   
   # Restaurar banco (se fez backup)
   psql -U seu_usuario -d zendesk_evaluator < backup_YYYYMMDD_HHMMSS.sql
   ```

---

## ✨ Próximos Passos Recomendados

1. Testar todas as funcionalidades em ambiente de desenvolvimento
2. Fazer testes de carga com múltiplos usuários
3. Verificar se os emails estão sendo enviados corretamente
4. Treinar a equipe nas novas funcionalidades
5. Coletar feedback dos avaliadores após 1 semana de uso

---

**Desenvolvido com ❤️ para Zendesk Evaluator**
