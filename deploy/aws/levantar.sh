#!/usr/bin/env bash
# Configura y arranca DFSha en las tres instancias de AWS. Se corre desde tu máquina:
#
#   deploy/aws/levantar.sh
#
# Sirve para la primera vez y para cada sesión nueva del laboratorio: las IP públicas
# cambian cuando una instancia se apaga y se enciende, y los ControlNodes tienen que
# anunciar las nuevas. Correrlo dos veces seguidas no cambia nada.
#
# Qué hace, en orden:
#   1. busca las instancias dfsha-nodo-0, -1 y -2, y enciende las que estén apagadas;
#   2. en cada una deja el repo al día y construye la imagen;
#   3. crea los secretos en el nodo 0 si no existen, y copia a los otros dos solo las
#      vistas que montan los contenedores (la llave de la CA no sale del nodo 0);
#   4. escribe el .env de cada instancia con las IP privadas y públicas, y arranca;
#   5. espera a que los seis nodos estén sanos y muestra el estado del clúster;
#   6. deja en tu máquina lo que necesita el cliente y dice cómo entrar.
#
# Necesita: AWS CLI con las credenciales de la sesión, y la llave SSH de las instancias.
# Variables opcionales: DFSHA_SSH_KEY, DFSHA_CLIENT_DIR, AWS_DEFAULT_REGION, DFSHA_REPO,
# DFSHA_BRANCH, y los parámetros del clúster (DFSHA_BLOCK_MB, DFSHA_HEARTBEAT_INTERVAL_S,
# DFSHA_DATANODE_DEAD_AFTER_S, DFSHA_REREPLICATION_INTERVAL_S, DFSHA_REREPLICATION_DELAY_S,
# DFSHA_GC_INTERVAL_S, DFSHA_GC_GRACE_S), que si están definidos van a las tres instancias.

set -euo pipefail

KEY="${DFSHA_SSH_KEY:-$HOME/.ssh/dfsha-aws.pem}"
CLIENT_DIR="${DFSHA_CLIENT_DIR:-$HOME/dfsha-aws}"
REPO="${DFSHA_REPO:-https://github.com/JeanArdila711/proyecto1-sd}"
BRANCH="${DFSHA_BRANCH:-main}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" AWS_PAGER=""
TUNABLES=(DFSHA_BLOCK_MB DFSHA_HEARTBEAT_INTERVAL_S DFSHA_DATANODE_DEAD_AFTER_S
          DFSHA_REREPLICATION_INTERVAL_S DFSHA_REREPLICATION_DELAY_S DFSHA_GC_INTERVAL_S DFSHA_GC_GRACE_S)

[ -r "$KEY" ] || { echo "no encuentro la llave SSH en $KEY (DFSHA_SSH_KEY)" >&2; exit 1; }

en() {  # en <ip> <comando>: corre un comando en una instancia
    ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o LogLevel=ERROR "ubuntu@$1" "$2"
}

echo "1. Instancias"
ids=()
for n in 0 1 2; do
    found=$(aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=dfsha-nodo-$n" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text)
    if [ "$(printf '%s\n' "$found" | grep -c .)" != 1 ]; then
        echo "tiene que haber exactamente una instancia llamada dfsha-nodo-$n; encontré: ${found:-ninguna}" >&2
        exit 1
    fi
    read -r id state <<<"$found"
    if [ "$state" = stopping ]; then aws ec2 wait instance-stopped --instance-ids "$id"; state=stopped; fi
    if [ "$state" = stopped ]; then
        echo "   dfsha-nodo-$n ($id) estaba apagada: la enciendo"
        aws ec2 start-instances --instance-ids "$id" >/dev/null
    fi
    ids+=("$id")
done
aws ec2 wait instance-running --instance-ids "${ids[@]}"

private=() public=()
for n in 0 1 2; do
    read -r priv pub <<<"$(aws ec2 describe-instances --instance-ids "${ids[$n]}" \
        --query 'Reservations[0].Instances[0].[PrivateIpAddress,PublicIpAddress]' --output text)"
    private+=("$priv") public+=("$pub")
    echo "   dfsha-nodo-$n  ${ids[$n]}  privada $priv  pública $pub"
done

echo "2. Código e imagen en cada instancia"
for n in 0 1 2; do
    ip="${public[$n]}"
    ready=""
    for _ in $(seq 1 40); do  # recién encendida, SSH y Docker tardan uno o dos minutos
        if en "$ip" 'docker compose version' >/dev/null 2>&1; then ready=1; break; fi
        sleep 6
    done
    if [ -z "$ready" ]; then
        echo "no logro entrar por SSH a dfsha-nodo-$n ($ip) o Docker no está listo." >&2
        echo "Si tu IP cambió, agrégala al security group (deploy/aws/README.md)." >&2
        exit 1
    fi
    en "$ip" "set -e
        if [ -d proyecto1-sd/.git ]; then
            git -C proyecto1-sd fetch -q origin '$BRANCH' && git -C proyecto1-sd checkout -q -f -B '$BRANCH' 'origin/$BRANCH'
        else
            git clone -q -b '$BRANCH' '$REPO' proyecto1-sd
        fi
        mkdir -p proyecto1-sd/intercambio
        docker build -q -t dfsha:local proyecto1-sd >/dev/null
        echo \"   dfsha-nodo-$n: \$(git -C proyecto1-sd log --oneline -1 | cut -c1-70)\""
done

echo "3. Secretos"
en "${public[0]}" 'set -e; cd proyecto1-sd
    docker run --rm -u 0:0 -v "$PWD/secrets:/secrets" -v "$PWD/intercambio:/intercambio" dfsha:local \
        python scripts/generate_secrets.py --dir /secrets --owner 1000:1000 --writable-dir /intercambio >/dev/null'
want=$(en "${public[0]}" 'cd proyecto1-sd/secrets/mounts && find . -type f | sort | xargs sha256sum | sha256sum')
for n in 1 2; do
    have=$(en "${public[$n]}" 'cd proyecto1-sd/secrets/mounts 2>/dev/null && find . -type f | sort | xargs sha256sum | sha256sum' || true)
    if [ "$have" = "$want" ]; then
        echo "   dfsha-nodo-$n ya tiene las mismas vistas que el nodo 0"
    else
        en "${public[0]}" 'tar -C proyecto1-sd -cz secrets/mounts' | en "${public[$n]}" 'tar -C proyecto1-sd -xz'
        echo "   dfsha-nodo-$n: vistas copiadas desde el nodo 0"
    fi
done

echo "4. Configuración y arranque"
for n in 0 1 2; do
    {
        echo "# Escrito por deploy/aws/levantar.sh. No se edita a mano."
        echo "DFSHA_NODE=$n"
        echo "DFSHA_DN=$((n + 1))"
        for i in 0 1 2; do echo "DFSHA_IP_$i=${private[$i]}"; done
        for i in 0 1 2; do echo "DFSHA_PUBLIC_$i=${public[$i]}"; done
        for name in "${TUNABLES[@]}"; do
            if [ -n "${!name:-}" ]; then echo "$name=${!name}"; fi
        done
    } | en "${public[$n]}" 'cat > proyecto1-sd/deploy/aws/.env'
    en "${public[$n]}" "cd proyecto1-sd/deploy/aws
        if ! docker compose up -d > ~/dfsha-up.log 2>&1; then tail -20 ~/dfsha-up.log; exit 1; fi
        echo '   dfsha-nodo-$n: arrancado'"
done

echo "5. Comprobación"
for n in 0 1 2; do
    healthy=""
    for _ in $(seq 1 40); do
        count=$(en "${public[$n]}" 'cd proyecto1-sd/deploy/aws && docker compose ps --format "{{.Status}}" | grep -c "(healthy)"' || true)
        if [ "$count" = 2 ]; then healthy=1; break; fi
        sleep 3
    done
    if [ -z "$healthy" ]; then
        echo "en dfsha-nodo-$n no quedaron sanos el ControlNode y el DataNode:" >&2
        en "${public[$n]}" 'cd proyecto1-sd/deploy/aws && docker compose ps -a; docker compose logs --tail 15' >&2
        exit 1
    fi
done
en "${public[0]}" 'cd proyecto1-sd/deploy/aws && docker compose run --rm -T inspect estado 2>/dev/null' | sed 's/^/   /'

echo "6. Cliente"
mkdir -p "$CLIENT_DIR/cliente"
en "${public[0]}" 'cat proyecto1-sd/secrets/mounts/client/ca.crt' > "$CLIENT_DIR/cliente/ca.crt"
(umask 077; en "${public[0]}" 'cat proyecto1-sd/secrets/admin.password' > "$CLIENT_DIR/admin.password")
cat > "$CLIENT_DIR/cliente.env" <<EOF
# Escrito por deploy/aws/levantar.sh: cambia en cada sesión del laboratorio.
DFSHA_CONTROL_NODES=${public[0]}:50051,${public[1]}:50051,${public[2]}:50051
DFSHA_CLIENT_SECRETS=$CLIENT_DIR/cliente
EOF

cat <<EOF

Listo. Para entrar desde esta máquina, en la carpeta del repo:

    docker compose --env-file "$CLIENT_DIR/cliente.env" run --rm cliente

y adentro: login admin    (contraseña inicial: cat "$CLIENT_DIR/admin.password")

Para ver el clúster por dentro:

    ssh -i "$KEY" ubuntu@${public[0]} 'cd proyecto1-sd/deploy/aws && docker compose run --rm -T inspect estado'
EOF
