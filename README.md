# QA Atendimentos · Zendesk

Plataforma de avaliação de atendimentos integrada ao Zendesk.

---

## 🚀 Subindo o servidor

```bash
npm install
node server.js
```

Acesse: **http://localhost:3001**

> Na primeira execução, os usuários e senhas são gerados automaticamente.

**Credenciais padrão:**
| Usuário | Senha |
|---------|-------|
| admin | admin123 |
| leticia, erika, gabriel... | senha123 |

---

## 🌐 Expondo o webhook publicamente com ngrok

O Zendesk precisa de uma URL HTTPS pública para enviar tickets.
Use o **ngrok** para criar um túnel da sua máquina para a internet.

### 1. Instalar o ngrok

```bash
# macOS (Homebrew)
brew install ngrok

# Windows (Chocolatey)
choco install ngrok

# Ou baixe direto em: https://ngrok.com/download
```

### 2. Criar conta gratuita e autenticar

```bash
# Crie conta em https://dashboard.ngrok.com/signup
# Copie seu authtoken e rode:
ngrok config add-authtoken SEU_TOKEN_AQUI
```

### 3. Abrir o túnel (em outro terminal)

Com o servidor já rodando (`node server.js`), abra um segundo terminal:

```bash
ngrok http 3001
```

Você verá algo como:

```
Forwarding  https://abc123.ngrok-free.app -> http://localhost:3001
```

### 4. Configurar o webhook no Zendesk

1. Acesse **Zendesk Admin → Apps and integrations → Webhooks**
2. Clique em **Create webhook**
3. Preencha:
   - **URL:** `https://abc123.ngrok-free.app/webhook/zendesk`
   - **Request method:** POST
   - **Request format:** JSON
4. Em **Triggers**, crie um trigger com condição:
   - Status **is** Solved
   - Ação: **Notify webhook** → selecione o webhook criado

> ⚠️ No plano gratuito do ngrok, a URL muda toda vez que você reinicia o túnel.
> Para URL fixa, use o plano pago ou faça deploy no Railway/Render.

---

## ☁️ Deploy permanente (URL fixa)

Para não depender do ngrok, faça deploy no **Railway** ou **Render**:

### Railway
```bash
# Instale a CLI
npm install -g @railway/cli

# Login e deploy
railway login
railway init
railway up
```

### Render
1. Suba o projeto para um repositório GitHub
2. Acesse https://render.com → **New Web Service**
3. Conecte o repositório e configure:
   - **Build command:** `npm install`
   - **Start command:** `node server.js`
4. A URL pública será gerada automaticamente

---

## 📁 Estrutura

```
zendesk-evaluator/
├── server.js          # Backend Express
├── init.js            # Geração manual de usuários (opcional)
├── package.json
├── public/
│   └── index.html     # Frontend SPA
└── data/
    ├── users.json
    ├── tickets.json
    └── evaluations.json
```
