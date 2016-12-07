# Quick Start for Python Google App Engine

## app.yaml

Deploys to `standard` or `flex` app engine.

## Simulators

```bash
gcloud beta emulators datastore start &
`gcloud beta emulators datastore env-init`

# emulators - start
gcloud beta emulators pubsub start &
`gcloud beta emulators pubsub env-init`

# emulators - kill
pkill -f emulators
unset PUBSUB_EMULATOR_HOST

```

## Setup

```
virtualenv venv

# windows
venv\scripts\activate.bat

# bash

pip install -r requirements.txt

python main.py

# deploy
gcloud app deploy --project=the-depot --version=v1 ./app.std.yaml

# browse to
(service) https://talk-demo-dot-<projectname>.appspot.com
(default) https://<projectname>.appspot.com


```

