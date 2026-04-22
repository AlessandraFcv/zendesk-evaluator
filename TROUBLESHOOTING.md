# 🔧 Troubleshooting - Servidor Não Inicia

## ❌ Erro Corrigido

**Problema**: `SyntaxError: Unexpected token '}'` na linha 1140  
**Causa**: `});` duplicado na rota `/api/tickets/:id/ai-review`  
**Status**: ✅ **CORRIGIDO** no arquivo `server.js` atualizado

---

## 🚀 Como Testar o Servidor

### 1️⃣ Verificar Sintaxe (antes de iniciar):
```bash
node -c server.js
```
- ✅ **Sem saída** = arquivo OK
- ❌ **Mostra erro** = tem problema de sintaxe

### 2️⃣ Iniciar o Servidor:
```bash
node server.js
```

### 3️⃣ Saídas Esperadas:
```
[INIT] Criando tabela tickets_descartados...
[INIT] Tabela tickets_descartados OK
[INIT] Migrando conducao -> conhecimento_produto...
[INIT] Migration OK
✓ Servidor rodando na porta 3000
```

---

## ⚠️ Outros Problemas Comuns

### Erro: "Cannot find module 'xxx'"
**Causa**: Falta instalar dependências  
**Solução**:
```bash
npm install express pg jsonwebtoken bcrypt nodemailer @google/generative-ai
```

### Erro: "Port 3000 already in use"
**Causa**: Outro processo usando a porta 3000  
**Solução**:
```bash
# Ver o processo
lsof -i :3000

# Matar o processo
kill -9 [PID]

# OU mudar a porta no server.js:
const PORT = process.env.PORT || 3001;
```

### Erro: "connect ECONNREFUSED 127.0.0.1:5432"
**Causa**: PostgreSQL não está rodando  
**Solução**:
```bash
# Linux/Mac
sudo service postgresql start

# Windows (usando PostgreSQL instalado)
net start postgresql-x64-XX
```

### Erro: "password authentication failed"
**Causa**: Credenciais do banco incorretas  
**Solução**:
- Verificar `.env` ou variáveis de ambiente
- Verificar configuração do PostgreSQL

---

## 🔍 Debug Avançado

### Ver logs detalhados:
```bash
NODE_ENV=development node server.js
```

### Testar funções específicas isoladamente:
```javascript
// Criar arquivo test.js
const { extractRejections } = require('./server.js');

const conversa = `
Cliente: Estou com erro
Rejeição: IE do emitente não cadastrada
`;

console.log(extractRejections(conversa));
// Deve retornar: ["IE do emitente não cadastrada"]
```

### Verificar se Help Center está acessível:
```bash
curl 'https://tecnospeed.zendesk.com/api/v2/help_center/articles/search.json?category=rejeição&query=teste'
```
- ✅ Deve retornar JSON com `results`
- ❌ Se der 404 ou erro, verificar subdomínio

---

## 📝 Checklist de Instalação

Antes de abrir o servidor, verifique:

- [ ] PostgreSQL está rodando
- [ ] Banco de dados `zendesk_evaluator` existe
- [ ] Dependências instaladas (`npm install`)
- [ ] Arquivo `.env` configurado (se usar)
- [ ] Sintaxe OK (`node -c server.js`)
- [ ] Porta 3000 disponível
- [ ] Node.js versão >= 14

---

## 🆘 Se o Problema Persistir

### Passos:
1. **Copie o erro completo** da tela
2. **Verifique a linha mencionada** no erro
3. **Compare com o arquivo original** (se tiver backup)
4. **Teste em partes**:
   ```bash
   # Comenta as rotas novas e testa
   # Descomenta uma por vez para isolar o problema
   ```

### Comandos úteis:
```bash
# Ver versão do Node
node -v

# Ver todas as portas em uso
netstat -tuln | grep LISTEN

# Ver processos Node rodando
ps aux | grep node

# Limpar cache do npm
npm cache clean --force
```

---

## ✅ Arquivo Corrigido

O arquivo `server.js` que você baixou agora:
- ✅ Não tem o `});` duplicado
- ✅ Todas as funções estão completas
- ✅ Sintaxe validada
- ✅ Pronto para rodar

**Baixe o novo `server.js` e substitua no seu projeto!**
