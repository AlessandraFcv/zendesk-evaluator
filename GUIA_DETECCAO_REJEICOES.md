# 🔍 Detecção Automática de Documentação Não Enviada

## 📋 Descrição

Esta funcionalidade monitora automaticamente quando um cliente menciona uma **rejeição** no atendimento e verifica se o consultor enviou a documentação disponível na base de conhecimento do Zendesk.

---

## 🎯 Como Funciona

### 1️⃣ Detecção de Rejeições
Quando a IA analisa um ticket, ela:
- **Escaneia** toda a conversa em busca de menções a rejeições
- **Padrão detectado**: `Rejeição: [descrição da rejeição]`
- **Exemplos**:
  - "Rejeição: IE do emitente não cadastrada"
  - "Rejeição 204: Duplicidade de NF-e"
  - "Rejeição: Data de emissão muito antiga"

### 2️⃣ Busca no Help Center
Para cada rejeição detectada:
- **Busca automática** no Help Center do Zendesk
- **Endpoint**: `https://tecnospeed.zendesk.com/api/v2/help_center/articles/search.json`
- **Par�metros**:
  - `category=rejeição` (categoria fixa)
  - `query=[texto da rejeição]` (extraído da conversa)

### 3️⃣ Verificação de Envio
- **Compara** os links encontrados com a conversa
- **Verifica** se o consultor enviou algum dos artigos ao cliente
- **Detecta** quando há documentação disponível MAS não foi enviada

### 4️⃣ Impacto na Análise
Se documentação disponível NÃO foi enviada:
- ✅ A IA **recebe essa informação** no contexto
- ✅ Deve mencionar como **PONTO DE MELHORIA significativo**
- ✅ Pode impactar a nota de **Conhecimento do Produto**
- ✅ Pode impactar a nota de **Solução**

---

## 📝 Exemplo Prático

### Conversa do Ticket:
```
[10/01/2025 14:30] Cliente:
Olá, estou tentando emitir uma nota e está dando erro:
Rejeição: IE do emitente não cadastrada

[10/01/2025 14:35] Consultor João Silva:
Olá! Tudo bem?
Essa rejeição acontece quando a Inscrição Estadual não está 
cadastrada na SEFAZ. Você precisa verificar no site da SEFAZ 
do seu estado se a IE está ativa.

[10/01/2025 14:40] Cliente:
Como faço isso?

[10/01/2025 14:42] Consultor João Silva:
Você precisa acessar o site da SEFAZ do seu estado e consultar 
a situação cadastral da empresa.
```

### O Que a IA Detecta:
1. **Rejeição mencionada**: "IE do emitente não cadastrada"
2. **Busca no Help Center** → Encontra 2 artigos:
   - "Como resolver: IE do emitente não cadastrada"
   - "Rejeições SEFAZ: Guia completo de soluções"
3. **Verifica conversa** → NÃO encontra os links dos artigos
4. **Conclusão**: Documentação disponível, mas não enviada

### Contexto Passado para a IA:
```
⚠️ IMPORTANTE - DOCUMENTAÇÃO DISPONÍVEL NÃO ENVIADA:

Identifiquei que havia documentação disponível na base de conhecimento 
que poderia ter sido enviada ao cliente:

Rejeição mencionada: "IE do emitente não cadastrada"
Documentação disponível:
- Como resolver: IE do emitente não cadastrada
  Link: https://tecnospeed.zendesk.com/hc/pt-br/articles/123456
- Rejeições SEFAZ: Guia completo de soluções
  Link: https://tecnospeed.zendesk.com/hc/pt-br/articles/789012

AÇÃO ESPERADA: O consultor deveria ter pesquisado e enviado esta 
documentação ao cliente para facilitar a resolução do problema.

IMPACTO NA AVALIAÇÃO: Este deve ser considerado um PONTO DE MELHORIA 
significativo na análise, pois havia recursos disponíveis que não 
foram utilizados.
```

### Análise da IA:
```json
{
  "resumo": "Eu avaliei este atendimento e identifiquei que o consultor 
           tentou ajudar, mas não utilizou os recursos disponíveis...",
  
  "pontos_positivos": [
    "Resposta rápida ao cliente",
    "Tom cordial e educado"
  ],
  
  "pontos_melhoria": [
    "⚠️ CRÍTICO: O cliente mencionou a rejeição 'IE do emitente não cadastrada' 
     e havia documentação completa disponível na base de conhecimento, mas o 
     consultor não enviou. Isso demonstra falta de pesquisa interna antes de 
     responder.",
    "Orientações genéricas sem enviar material de apoio",
    "Não utilizou os recursos da empresa para facilitar a vida do cliente"
  ],
  
  "scores": {
    "solucao": 50,
    "empatia": 75,
    "conhecimento_produto": 25
  },
  
  "nota_sugerida": 50,
  
  "justificativa": "Observei que havia documentação específica para esta 
                   rejeição que não foi compartilhada com o cliente, o que 
                   configura uma falha significativa no uso dos recursos 
                   disponíveis."
}
```

---

## 🔧 Implementação Técnica

### Funções Criadas:

#### 1. `extractRejections(conversa)`
```javascript
// Detecta padrões como "Rejeição: [descrição]"
// Retorna: Array de strings com as rejeições encontradas
// Exemplo: ["IE do emitente não cadastrada", "Data de emissão muito antiga"]
```

#### 2. `searchHelpCenterArticles(category, query, cfg)`
```javascript
// Busca artigos no Help Center público do Zendesk
// Par�metros:
//   - category: "rejeição" (fixo)
//   - query: texto da rejeição extraída
//   - cfg: configuração do Zendesk
// Retorna: Array de objetos { title, url, snippet }
```

### Integração nas Rotas:

#### `/api/tickets/:id/ai-review` (Análise Normal)
```javascript
// 1. Extrai rejeições da conversa
const rejeicoes = extractRejections(conversa);

// 2. Para cada rejeição, busca artigos
for (const rejeicao of rejeicoes) {
  const artigos = await searchHelpCenterArticles('rejeição', rejeicao, cfg);
  
  // 3. Verifica se consultor enviou os links
  const linksEnviados = artigos.some(artigo => 
    conversa.toLowerCase().includes(artigo.url.toLowerCase())
  );
  
  // 4. Registra documentação não enviada
  if (!linksEnviados) {
    documentacaoNaoEnviada.push({ rejeicao, artigos });
  }
}

// 5. Passa informação para a IA no contexto
```

#### `/api/csat/tickets/:id/ai-review` (Análise CSAT)
- **Mesma lógica** aplicada
- **Contexto adicional**: Pode ter contribuído para a insatisfação do cliente

---

## 📊 Benefícios

### Para Avaliadores:
- ✅ **Detecção automática** de falhas no uso de recursos internos
- ✅ **Análise mais precisa** com informações que o humano poderia não saber
- ✅ **Evidência objetiva** de documentação disponível

### Para Gestores:
- ✅ **Identificar gaps** no treinamento de consultores
- ✅ **Mapear** quais rejeições têm documentação mas não são compartilhadas
- ✅ **Melhorar processos** de busca e compartilhamento de conteúdo

### Para Consultores:
- ✅ **Feedback específico** sobre recursos disponíveis
- ✅ **Aprendizado** sobre onde buscar soluções
- ✅ **Melhoria contínua** na utilização da base de conhecimento

---

## ⚙️ Configuração

### Nenhuma configuração adicional necessária!

A funcionalidade:
- ✅ Funciona automaticamente quando a IA analisa tickets
- ✅ Usa o subdomínio Zendesk já configurado
- ✅ Busca no Help Center público (sem autenticação)
- ✅ Falhas silenciosas (não quebra a análise se a busca falhar)

### Log de Debug:
Mensagens no console do servidor:
```
[HELP CENTER SEARCH] HTTP 200 - Found 3 articles
[HELP CENTER SEARCH] Request error: timeout
[HELP CENTER SEARCH] Parse error: invalid JSON
```

---

## 🎯 Casos de Uso

### ✅ Detecta:
- Rejeições fiscais (NF-e, CT-e, MDF-e, etc.)
- Rejeições de integração
- Rejeições de validação
- Qualquer texto após "Rejeição:" ou "Rejeicao:"

### ⚠️ Limitações:
- Só detecta se estiver escrito explicitamente "Rejeição:"
- Não detecta se o cliente mencionar indiretamente
- Depende da qualidade da base de conhecimento
- Máximo de 3 artigos por rejeição (configura `artigos.slice(0, 3)`)

### 🔄 Casos Especiais:

#### Rejeição mencionada MAS link enviado:
```
Cliente: Rejeição: IE não cadastrada
Consultor: Veja este artigo: https://tecnospeed.zendesk.com/hc/...
```
✅ IA **NÃO** reclama (link foi enviado)

#### Múltiplas rejeições:
```
Cliente: Estou com 3 rejeições:
- Rejeição 204: Duplicidade
- Rejeição 301: Data inválida
- Rejeição 999: Teste
```
✅ IA busca as 3 separadamente e analisa cada uma

---

## 🚀 Próximas Melhorias (Futuro)

### Possíveis Expansões:
1. **Detectar outros padrões**: "Erro:", "Código:", etc.
2. **Buscar em múltiplas categorias**: Não só "rejeição"
3. **Scoring automático**: Deduzir pontos automaticamente
4. **Dashboard**: Mostrar quais rejeições têm mais documentação não enviada
5. **Sugestão proativa**: Sugerir artigos durante o atendimento (tempo real)

---

## ✨ Resumo

Essa funcionalidade transforma a IA de um simples analisador em um **auditor inteligente** que:
- 🔍 Detecta falhas invisíveis ao olho humano
- 📚 Conhece toda a base de conhecimento
- 🎯 Identifica oportunidades de melhoria objetivas
- 📊 Gera análises mais precisas e acionáveis

**Resultado**: Avaliações mais justas, feedback mais útil, e melhoria contínua real! 🚀
