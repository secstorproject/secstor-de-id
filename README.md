# DE-IDentification (de-id) 🌐🔏

Este serviço web Django utiliza a biblioteca secstor-anonymizer_lib para oferecer anonimização de dados como um serviço, contribuindo na conformidade com a Lei Geral de Proteção de Dados (LGPD).

O serviço permite que aplicações clientes solicitem anonimização de dados via uma interface RESTful.

## Recursos

- API RESTful para anonimização de dados.
- Suporte a múltiplas técnicas de anonimização.
- Segurança robusta para proteger os dados durante o processamento.

## Configuração do Ambiente

### Pré-Requisitos

- Python 3.6+
- Django 3.1+
- secstor-anonymizer_lib (já incluída no projeto)

### Executando

Na raiz do projeto execute:

```bash
  [...]
    python -m venv venv
    venv/scripts/activate
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    python manage.py makemigrations
    python manage.py migrate
    python manage.py runserver
  [...]
```

```bash
  [...]
    cd de-id
    celery -A config worker --pool=solo -l info
  [...]
```
---

👤 Contribuidor Principal: [losthunter52](https://github.com/losthunter52/de-id)