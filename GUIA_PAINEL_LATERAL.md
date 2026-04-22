# 📋 Guia do Painel Lateral de Critérios

## Como Funciona Agora (CORRIGIDO)

### 1️⃣ Durante a Avaliação
Quando o avaliador está avaliando um ticket, **APÓS a IA fazer a análise**, aparece um botão:

```
📋 Ver Critérios de Avaliação ▶
```

### 2️⃣ Ao Clicar no Botão
Um painel lateral **desliza da direita** no **MESMO PLANO VISUAL** da tela de avaliação.

**Comportamento:**
- ✅ Painel aparece à DIREITA
- ✅ Tela de avaliação PERMANECE VISÍVEL e FUNCIONAL
- ✅ NENHUM overlay escuro (não tem fundo escurecido)
- ✅ Ambos no mesmo nível visual (z-index: 1001)

### 3️⃣ Layout do Painel
```
┌────────────────────────────────┬──────────────────────┐
│  MODAL DE AVALIAÇÃO           │  PAINEL CRITÉRIOS    │
│  (permanece visível)          │  (desliza da direita)│
├────────────────────────────────┼──────────────────────┤
│                               │ 📋 Critérios [×]     │
│  [Conversa completa]          │──────────────────────│
│  [Análise da IA]              │                      │
│  😞 😕 🙂 😊 🤩 Solução        │  **Critério 1**      │
│  😞 😕 🙂 😊 🤩 Empatia        │  - Item A            │
│  😞 😕 🙂 😊 🤩 Conhecimento   │  - Item B            │
│                               │                      │
│  📋 Ver Critérios ▶  ────────▶│  **Critério 2**      │
│                               │  - Item C            │
│  [Observações]                │  - Item D            │
│  [💾 Salvar Avaliação]        │                      │
│                               │  **Critério 3**      │
│                               │  - Item E            │
│                               │                      │
│                               │  (scroll)            │
└────────────────────────────────┴──────────────────────┘
```

**Largura do painel:** 420px  
**Posição:** Fixo à direita da tela (não dentro do modal)  
**Animação:** Desliza suavemente (0.3s)  
**z-index:** 1001 (logo acima do modal que é 1000)

### 4️⃣ Como Fechar
O avaliador pode fechar o painel de **2 formas**:
- ✅ Clicando no **[×]** no canto superior direito do painel
- ✅ Pressionando **ESC** no teclado

**NÃO fecha mais:**
- ❌ Clicando fora (não tem overlay)

### 5️⃣ O Que Acontece ao Fechar
- O painel **desliza de volta para a direita** (animação suave)
- A tela de avaliação **permanece aberta** (não fecha!)
- O avaliador pode continuar ajustando as notas

---

## 🎯 Vantagens Deste Modelo

### ✅ Comparação Lado a Lado
O avaliador pode:
1. Abrir o painel de critérios → desliza da direita
2. Ver os critérios na **lateral direita**
3. Ajustar as notas dos emojis à **esquerda**
4. **Não precisa memorizar** os critérios
5. **Trabalha nos dois ao mesmo tempo** (sem overlay bloqueando)

### ✅ Workflow Fluido
```
Fluxo antigo (modal bloqueante):
1. Ver análise IA
2. Clicar "Ver Critérios"
3. ❌ Tela de avaliação some
4. Ler critérios
5. Fechar modal
6. ❌ Precisa lembrar dos critérios
7. Ajustar notas

Fluxo NOVO (drawer lateral):
1. Ver análise IA
2. Clicar "Ver Critérios ▶"
3. ✅ Painel desliza da direita
4. ✅ Avaliação permanece VISÍVEL e FUNCIONAL
5. ✅ Lê critérios ENQUANTO ajusta notas
6. ✅ Clica nos emojis à esquerda
7. ✅ Consulta critérios à direita
8. Fechar painel quando terminar
```

### ✅ Sem Bloqueio Visual
- Antes: Overlay escuro cobria a tela → precisava fechar para ver
- Agora: Sem overlay → vê os dois ao mesmo tempo

---

## 🎨 Detalhes Técnicos

### CSS Principais
```css
.criteria-drawer {
  position: fixed;
  top: 58px;              /* Abaixo da topbar */
  right: 0;
  width: 420px;
  height: calc(100vh - 58px);
  transform: translateX(100%);  /* Escondido à direita */
  transition: transform .3s ease;
  z-index: 1001;          /* Logo acima do modal (1000) */
  box-shadow: -8px 0 24px rgba(0,0,0,.3);  /* Sombra à esquerda */
}

.criteria-drawer.open {
  transform: translateX(0);  /* Desliza para dentro */
}
```

### JavaScript
```javascript
// Abre o drawer
async function showCriteriaModal() {
  const drawer = document.getElementById('criteria-drawer');
  drawer.classList.add('open');
  // ... carrega critérios
}

// Fecha o drawer
function closeCriteriaDrawer() {
  const drawer = document.getElementById('criteria-drawer');
  drawer.classList.remove('open');
}

// Tecla ESC também fecha
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && drawer.classList.contains('open')) {
    closeCriteriaDrawer();
  }
});
```

---

## ✨ Resultado Final

O avaliador agora pode:
1. ✅ Ver a análise da IA
2. ✅ Abrir critérios **ao lado** (não em cima)
3. ✅ Comparar critérios enquanto ajusta notas **SEM BLOQUEIO**
4. ✅ Trabalhar nos dois simultaneamente
5. ✅ Fechar quando quiser (X ou ESC)
6. ✅ Continuar avaliando sem perder contexto

**Produtividade MAXIMIZADA!** 🚀

