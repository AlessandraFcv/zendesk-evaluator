/**
 * EXECUTE ESTE SCRIPT UMA VEZ para gerar as senhas corretamente:
 *   node init.js
 */
const bcrypt = require('bcryptjs');
const fs     = require('fs');
const path   = require('path');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

async function init() {
  console.log('\n🔐 Gerando hashes de senha...\n');

  const users = [
    { id:'admin',      username:'admin',      name:'Administrador', role:'admin',      assignee_ids:[],                                 plain:'admin123'  },
    { id:'leticia',    username:'leticia',    name:'Leticia',       role:'consultant', assignee_ids:['30235984721815'],                  plain:'senha123'  },
    { id:'willian',    username:'willian',    name:'Willian',       role:'consultant', assignee_ids:['6331924167191','27566434868503'],   plain:'senha123'  },
    { id:'erika',      username:'erika',      name:'Erika',         role:'consultant', assignee_ids:['34323495409943'],                  plain:'senha123'  },
    { id:'hiziane',    username:'hiziane',    name:'Hiziane',       role:'consultant', assignee_ids:['34323515216791'],                  plain:'senha123'  },
    { id:'gabriel',    username:'gabriel',    name:'Gabriel',       role:'consultant', assignee_ids:['27566549990295'],                  plain:'senha123'  },
    { id:'marcelo',    username:'marcelo',    name:'Marcelo',       role:'consultant', assignee_ids:['1529827574762'],                   plain:'senha123'  },
    { id:'henrique',   username:'henrique',   name:'Henrique',      role:'consultant', assignee_ids:['27566497005975'],                  plain:'senha123'  },
    { id:'iara',       username:'iara',       name:'Iara',          role:'consultant', assignee_ids:['34991041944855'],                  plain:'senha123'  },
    { id:'alessandra', username:'alessandra', name:'Alessandra',    role:'consultant', assignee_ids:['412356865654'],                   plain:'senha123'  },
    { id:'emilly',     username:'emilly',     name:'Emilly',        role:'consultant', assignee_ids:['23339270934935'],                  plain:'senha123'  },
    { id:'gilberto',   username:'gilberto',   name:'Gilberto',      role:'consultant', assignee_ids:['424200531334'],                   plain:'senha123'  },
  ];

  const hashed = await Promise.all(users.map(async ({ plain, ...u }) => {
    const password = await bcrypt.hash(plain, 10);
    console.log(`  ✅ ${u.username.padEnd(14)} hash gerado`);
    return { ...u, password };
  }));

  fs.writeFileSync(path.join(DATA_DIR, 'users.json'),       JSON.stringify(hashed,  null, 2));
  if (!fs.existsSync(path.join(DATA_DIR, 'tickets.json')))     fs.writeFileSync(path.join(DATA_DIR, 'tickets.json'),     '[]');
  if (!fs.existsSync(path.join(DATA_DIR, 'evaluations.json'))) fs.writeFileSync(path.join(DATA_DIR, 'evaluations.json'), '[]');
  if (!fs.existsSync(path.join(DATA_DIR, 'config.json')))      fs.writeFileSync(path.join(DATA_DIR, 'config.json'),      '{}');

  console.log('\n✅ Pronto! data/users.json gerado com senhas reais.');
  console.log('\n📋 Credenciais:');
  console.log('   admin        → admin123');
  console.log('   consultores  → senha123\n');
  console.log('🚀 Agora rode: node server.js\n');
}

init().catch(console.error);
