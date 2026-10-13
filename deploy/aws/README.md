# DFSha en AWS

Cómo levantar el clúster en tres instancias EC2 y entrar desde otra máquina por Internet. Los comandos son los que se usaron el 2026-10-13 en una cuenta de AWS Academy (Learner Lab), región `us-east-1`.

## Qué se despliega

Tres instancias, cada una con un ControlNode y un DataNode:

| Instancia | Nodos | Tipo usado |
|---|---|---|
| `dfsha-nodo-0` | `cn0` y `dn1` | `t3.medium` |
| `dfsha-nodo-1` | `cn1` y `dn2` | `t3.small` |
| `dfsha-nodo-2` | `cn2` y `dn3` | `t3.small` |

Con esto, perder una máquina entera deja dos ControlNodes, que son mayoría para Raft, y dos DataNodes, que alcanzan para leer y para escribir.

**Cómo se hablan los nodos.** Los nombres de siempre (`cn0`, `dn1`, ...) siguen siendo la identidad de cada nodo. En cada instancia, los contenedores usan la red de la instancia y resuelven esos nombres a la IP privada de la máquina donde vive cada uno (`deploy/aws/docker-compose.yml`). Raft, el pipeline de réplicas, la re-replicación y el recolector van por las IP privadas.

**Cómo entra un cliente.** Por las IP públicas. Cada ControlNode anuncia la IP pública de cada DataNode, y el cliente solo necesita la lista de los tres ControlNodes y el certificado de la CA.

| Puerto | Qué es | Quién puede llegar |
|---|---|---|
| 22 | SSH | solo tu IP |
| 50051 | ControlNode | los clientes, y las instancias entre sí |
| 50061 | DataNode | los clientes, y las instancias entre sí |
| 6000 | Raft | solo las instancias entre sí |

## Antes de empezar

- AWS CLI v2 con las credenciales de la sesión del laboratorio. En AWS Academy se copian de *AWS Details* a `~/.aws/credentials`, duran lo que dura la sesión y cambian en cada una.
- Docker en tu máquina, para el cliente.
- `bash`, para `levantar.sh`. En Windows sirve Git Bash o WSL.

```bash
aws sts get-caller-identity --region us-east-1     # tiene que responder con tu usuario
export AWS_DEFAULT_REGION=us-east-1
```

## 1. Una sola vez: llave, security group e instancias

```bash
aws ec2 create-key-pair --key-name dfsha-key --key-type ed25519 \
    --query KeyMaterial --output text > ~/.ssh/dfsha-aws.pem
chmod 600 ~/.ssh/dfsha-aws.pem

MI_IP=$(curl -s https://checkip.amazonaws.com)
SG=$(aws ec2 create-security-group --group-name dfsha-cluster \
    --description "DFSha: SSH y puertos de cliente" --query GroupId --output text)

# Desde tu IP: SSH y los puertos de cliente.
aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MI_IP/32,Description=ssh}]" \
    "IpProtocol=tcp,FromPort=50051,ToPort=50051,IpRanges=[{CidrIp=$MI_IP/32,Description=controlnodes}]" \
    "IpProtocol=tcp,FromPort=50061,ToPort=50061,IpRanges=[{CidrIp=$MI_IP/32,Description=datanodes}]"

# Entre las instancias del clúster: Raft y gRPC. El origen es el mismo security group.
aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=6000,ToPort=6000,UserIdGroupPairs=[{GroupId=$SG,Description=raft-entre-nodos}]" \
    "IpProtocol=tcp,FromPort=50051,ToPort=50051,UserIdGroupPairs=[{GroupId=$SG,Description=controlnodes-entre-nodos}]" \
    "IpProtocol=tcp,FromPort=50061,ToPort=50061,UserIdGroupPairs=[{GroupId=$SG,Description=datanodes-entre-nodos}]"
```

Ubuntu 24.04, con Docker instalado en el primer arranque. El nombre de cada instancia es lo que usa `levantar.sh` para encontrarla:

```bash
AMI=$(aws ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)
printf '#!/bin/bash\ncurl -fsSL https://get.docker.com | sh\nusermod -aG docker ubuntu\n' > userdata.sh

for n in 0 1 2; do
    aws ec2 run-instances --image-id "$AMI" --instance-type t3.small \
        --key-name dfsha-key --security-group-ids "$SG" --user-data file://userdata.sh \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp3}' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=dfsha-nodo-$n}]" \
        --query 'Instances[0].InstanceId' --output text
done
```

Para que entre otra persona se agrega su IP a los dos puertos de cliente, nunca al 22:

```bash
aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=50051,ToPort=50051,IpRanges=[{CidrIp=<su IP>/32}]" \
    "IpProtocol=tcp,FromPort=50061,ToPort=50061,IpRanges=[{CidrIp=<su IP>/32}]"
```

## 2. Levantar el clúster

Desde la carpeta del repo, en tu máquina:

```bash
deploy/aws/levantar.sh
```

El script busca las tres instancias, enciende las que estén apagadas, deja el repo al día en cada una y construye la imagen, crea los secretos en el nodo 0 si no existen y copia a los otros dos solo lo que montan sus contenedores, escribe la configuración de cada instancia con las IP privadas y públicas, arranca, y espera a que los seis nodos estén sanos. Al final muestra el estado del clúster y deja en `~/dfsha-aws/` lo que necesita el cliente.

Se puede correr las veces que haga falta: si nada cambió, no cambia nada.

Para una demo, con bloques de 1 MB y fallos que se detectan y reparan en segundos:

```bash
DFSHA_BLOCK_MB=1 DFSHA_HEARTBEAT_INTERVAL_S=0.5 DFSHA_DATANODE_DEAD_AFTER_S=2 \
DFSHA_REREPLICATION_INTERVAL_S=1 DFSHA_REREPLICATION_DELAY_S=2 \
DFSHA_GC_INTERVAL_S=5 DFSHA_GC_GRACE_S=5 deploy/aws/levantar.sh
```

Sin esas variables el clúster usa los valores normales (bloques de 128 MB).

## 3. Entrar desde tu máquina

```bash
docker compose --env-file ~/dfsha-aws/cliente.env run --rm cliente
# dfsha:/$ login admin               la contraseña inicial: cat ~/dfsha-aws/admin.password
# dfsha:/$ send /intercambio/tesis.pdf /docs/tesis.pdf
```

El servicio `cliente` no entra a la red del clúster ni levanta nodos: solo necesita llegar a los puertos 50051 y 50061 de las tres instancias. Otra persona necesita `~/dfsha-aws/cliente/ca.crt`, las tres direcciones de `cliente.env` y su IP en el security group.

El inspector corre dentro del clúster, desde cualquiera de las tres instancias:

```bash
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@<IP pública de una instancia>
cd proyecto1-sd/deploy/aws
docker compose run --rm inspect estado          # líder y nodos vivos
docker compose run --rm inspect mapa            # qué bloque está en qué DataNode
docker compose run --rm inspect bloques /docs/tesis.pdf
```

## 4. En cada sesión nueva del laboratorio

```bash
deploy/aws/levantar.sh
```

Una instancia que se apaga y se enciende conserva su IP privada y su disco, pero vuelve con otra IP pública. Los nodos están configurados para arrancar solos al encender la máquina (`restart: unless-stopped`) y entre ellos usan las IP privadas, así que el clúster debería armarse sin ayuda; lo que queda viejo es la IP pública que anuncian. El script la actualiza y reescribe `~/dfsha-aws/cliente.env`. Si tu propia IP cambió, hay que agregarla al security group.

Este arranque con IP pública nueva no se probó todavía.

## 5. Probar que tolera la caída de una máquina

```bash
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@<IP del nodo 0> 'cd proyecto1-sd/deploy/aws && docker compose stop'
docker compose --env-file ~/dfsha-aws/cliente.env run --rm cliente      # sigue leyendo y escribiendo
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@<IP del nodo 1> 'cd proyecto1-sd/deploy/aws && docker compose run --rm inspect estado'
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@<IP del nodo 0> 'cd proyecto1-sd/deploy/aws && docker compose start'
```

`docker compose stop` apaga el ControlNode y el DataNode de esa máquina sin tocar la instancia. Apagar la instancia de verdad (`aws ec2 stop-instances`) cambia además su IP pública: después hay que correr `levantar.sh`.

## Qué se comprobó el 2026-10-13

Con el cliente en un Mac, por Internet, y los valores de demo:

- `levantar.sh` dejó los seis nodos sanos: `cn0` líder, `cn1` y `cn2` seguidores, los tres DataNodes vivos.
- `send` de un archivo de 3,5 MB (4 bloques), `ls -l` y `receive`: idéntico al original. El inspector muestra cada bloque con sus tres réplicas `ok`, una en cada máquina, y la cabeza del pipeline rotando entre las tres instancias.
- Desde fuera responden el 50051 y el 50061 de las tres instancias. El 6000 de Raft no responde en ninguna.
- Con el ControlNode líder y el DataNode de `dfsha-nodo-0` apagados: `cn1` tomó el liderazgo en otra máquina, y el cliente leyó el archivo anterior y subió y bajó uno nuevo, todo idéntico. La sesión completa del cliente tardó 6 segundos.
- Al encender esos dos nodos: `cn0` volvió como seguidor, y el archivo escrito mientras faltaban recuperó su tercera réplica en `dn1`.
- `secrets/` quedó con dueño `1000:1000`, el usuario de la imagen, y la llave de la CA con modo `600`. En macOS esto no se podía comprobar.
- El reloj de la instancia estaba sincronizado por NTP. Importa porque las capabilities vencen según el reloj del DataNode.

Lo que no se probó: apagar una instancia entera desde AWS, y el arranque con las IP públicas cambiadas. La caída de la máquina se probó deteniendo sus dos contenedores; un nodo detenido rechaza la conexión al instante, y una instancia apagada no responde, así que el cliente puede tardar más en pasar a otra réplica.

## Alternativa: todo en una instancia

Las seis piezas en una sola máquina, con `docker-compose.publico.yml` de la raíz del repo. Es más simple y fue lo primero que se desplegó, pero no tolera la caída de la máquina.

```bash
# en la instancia
git clone https://github.com/JeanArdila711/proyecto1-sd && cd proyecto1-sd
DFSHA_PUBLIC_HOST=<IP pública de la instancia> \
    docker compose -f docker-compose.yml -f docker-compose.publico.yml up -d --build

# en tu máquina, con el ca.crt de ese clúster en ~/dfsha-aws/cliente/
DFSHA_CONTROL_NODES=<IP>:50051,<IP>:50052,<IP>:50053 DFSHA_CLIENT_SECRETS=~/dfsha-aws/cliente \
    docker compose run --rm cliente
```

Ahí cada nodo sale por un puerto distinto de la misma máquina (50051 a 50053 y 50061 a 50063), y esos son los rangos que hay que abrir en el security group.

## Apagar y borrar

```bash
aws ec2 stop-instances --instance-ids <id> <id> <id>         # conserva los discos
aws ec2 terminate-instances --instance-ids <id> <id> <id>    # borra las instancias y sus datos
aws ec2 delete-security-group --group-name dfsha-cluster     # después de terminar las instancias
aws ec2 delete-key-pair --key-name dfsha-key
```

## Límites

- **Las IP públicas cambian** cada vez que una instancia se apaga y se enciende. Una Elastic IP por instancia las dejaría fijas; no se usaron, y antes de usarlas hay que revisar cuánto consumen del presupuesto del laboratorio.
- **Los secretos se reparten por SSH** desde el nodo 0, pasando por la máquina de quien corre el script. La llave de la CA se queda en el nodo 0.
- **Una sola zona de disponibilidad.** Las tres instancias quedaron en `us-east-1d`: toleran la caída de una máquina, no la de la zona.
- **Tres nodos fijos.** El archivo de Compose y el script asumen tres instancias con un ControlNode y un DataNode cada una.
- **El security group solo deja entrar a las IP que se agreguen.** Para una demo con público hay que agregar cada IP, o abrir los dos puertos de cliente a `0.0.0.0/0` mientras dure.
