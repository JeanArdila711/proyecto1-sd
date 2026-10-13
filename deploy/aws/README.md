# DFSha en AWS

Cómo levantar el clúster en una instancia EC2 y entrar desde otra máquina por Internet. Los comandos son los que se usaron el 2026-10-13 en una cuenta de AWS Academy (Learner Lab), región `us-east-1`.

## Qué se despliega

Una sola instancia con las seis piezas en contenedores: tres ControlNodes y tres DataNodes, igual que en local. Lo que cambia es que cada nodo publica un puerto en la instancia y que los ControlNodes le entregan a un cliente de fuera la dirección pública de cada DataNode (`docker-compose.publico.yml`).

Lo que esto demuestra: el sistema completo corriendo en la nube, con TLS, autenticación y permisos, usado desde otra máquina por Internet. Lo que no demuestra: tolerancia a la caída de una máquina. Con todo en una instancia, si la instancia se apaga se apaga el clúster. Repartir los nodos en tres instancias es el paso siguiente y no está hecho.

| Puerto | Qué es | Se abre a |
|---|---|---|
| 22 | SSH | solo tu IP |
| 50051, 50052, 50053 | ControlNodes `cn0`, `cn1`, `cn2` | los clientes |
| 50061, 50062, 50063 | DataNodes `dn1`, `dn2`, `dn3` | los clientes |
| 6000 | Raft entre ControlNodes | nadie: no se publica |

## Antes de empezar

- AWS CLI v2 con las credenciales de la sesión del laboratorio. En AWS Academy se copian de *AWS Details* a `~/.aws/credentials`, duran lo que dura la sesión (4 horas) y cambian en cada una.
- Docker en tu máquina, para el cliente.

```bash
aws sts get-caller-identity --region us-east-1     # tiene que responder con tu usuario
export AWS_DEFAULT_REGION=us-east-1
```

## 1. Llave SSH y security group

```bash
aws ec2 create-key-pair --key-name dfsha-key --key-type ed25519 \
    --query KeyMaterial --output text > ~/.ssh/dfsha-aws.pem
chmod 600 ~/.ssh/dfsha-aws.pem

MI_IP=$(curl -s https://checkip.amazonaws.com)
SG=$(aws ec2 create-security-group --group-name dfsha-cluster \
    --description "DFSha: SSH y puertos de cliente" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MI_IP/32,Description=ssh}]" \
    "IpProtocol=tcp,FromPort=50051,ToPort=50053,IpRanges=[{CidrIp=$MI_IP/32,Description=controlnodes}]" \
    "IpProtocol=tcp,FromPort=50061,ToPort=50063,IpRanges=[{CidrIp=$MI_IP/32,Description=datanodes}]"
```

Así el clúster solo responde a tu IP. Para que entre otra persona, se agrega su IP a los dos rangos de cliente, nunca al 22:

```bash
aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=50051,ToPort=50053,IpRanges=[{CidrIp=<su IP>/32}]" \
    "IpProtocol=tcp,FromPort=50061,ToPort=50063,IpRanges=[{CidrIp=<su IP>/32}]"
```

## 2. La instancia

Ubuntu 24.04, `t3.medium` (2 vCPU, 4 GB), disco de 20 GB. Docker se instala solo en el primer arranque.

```bash
AMI=$(aws ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)

printf '#!/bin/bash\ncurl -fsSL https://get.docker.com | sh\nusermod -aG docker ubuntu\n' > userdata.sh

IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t3.medium \
    --key-name dfsha-key --security-group-ids "$SG" --user-data file://userdata.sh \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp3}' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dfsha-cluster}]' \
    --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$IID"
IP=$(aws ec2 describe-instances --instance-ids "$IID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "$IP"
```

Docker tarda uno o dos minutos más en quedar instalado. Para comprobarlo:

```bash
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@$IP 'cloud-init status; docker compose version'
# status: done
```

## 3. El clúster

En la instancia:

```bash
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@$IP
git clone https://github.com/JeanArdila711/proyecto1-sd && cd proyecto1-sd
DFSHA_PUBLIC_HOST=<IP pública de la instancia> \
    docker compose -f docker-compose.yml -f docker-compose.publico.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.publico.yml ps     # 6 servicios healthy
```

El 2026-10-13 el código se subió con `git archive` y `scp` en vez de `git clone`, porque el cambio todavía no estaba en `main`; el resto fue igual.

`DFSHA_PUBLIC_HOST` es la dirección por la que los clientes llegan a la instancia. Sin ella Compose no arranca y dice qué falta. La imagen se construye en la instancia, que es x86_64; no sirve una construida en un Mac con Apple Silicon.

La primera vez, `init` crea `secrets/` en la instancia. De ahí salen las dos cosas que necesita un cliente: el certificado de la CA y la contraseña inicial del `admin`.

## 4. Entrar desde tu máquina

```bash
mkdir -p ~/dfsha-aws/cliente
scp -i ~/.ssh/dfsha-aws.pem ubuntu@$IP:proyecto1-sd/secrets/mounts/client/ca.crt ~/dfsha-aws/cliente/
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@$IP 'cat proyecto1-sd/secrets/admin.password'
```

Desde la carpeta del repo, en tu máquina:

```bash
DFSHA_CONTROL_NODES=$IP:50051,$IP:50052,$IP:50053 DFSHA_CLIENT_SECRETS=~/dfsha-aws/cliente \
    docker compose run --rm cliente
# dfsha:/$ login admin
# dfsha:/$ send /intercambio/tesis.pdf /docs/tesis.pdf
```

En PowerShell las variables van antes, con `$env:DFSHA_CONTROL_NODES = "..."` y `$env:DFSHA_CLIENT_SECRETS = "..."`.

El servicio `cliente` no entra a la red del clúster ni levanta nodos: solo necesita llegar a los seis puertos. El inspector sí corre dentro de la red, así que se usa en la instancia:

```bash
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@$IP
cd proyecto1-sd
DFSHA_PUBLIC_HOST=<IP> docker compose -f docker-compose.yml -f docker-compose.publico.yml run --rm inspect mapa
```

## 5. En cada sesión nueva del laboratorio

Al terminar la sesión, el laboratorio detiene la instancia. Los datos y los secretos quedan en el disco, pero una instancia que se apaga y se enciende vuelve con otra IP pública, y los ControlNodes siguen anunciando la vieja. Hay que volver a levantarlos con la nueva:

```bash
aws ec2 start-instances --instance-ids <id>          # si el laboratorio no la encendió
aws ec2 describe-instances --instance-ids <id> \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
ssh -i ~/.ssh/dfsha-aws.pem ubuntu@<IP nueva>
cd proyecto1-sd
DFSHA_PUBLIC_HOST=<IP nueva> docker compose -f docker-compose.yml -f docker-compose.publico.yml up -d
```

Compose vuelve a crear los ControlNodes porque les cambió la configuración. Los archivos siguen ahí. Si tu propia IP cambió, hay que agregarla al security group como en el paso 1.

Este paso todavía no se probó, ni se comprobó en esta cuenta que el laboratorio detenga la instancia: la primera sesión terminó con el clúster recién levantado.

## Qué se comprobó el 2026-10-13

Con el clúster en una `t3.medium` y el cliente en un Mac, por Internet:

- Los seis servicios quedaron `healthy`.
- `login`, `mkdir`, `send` de un archivo de 2,2 MB, `ls -l` y `receive`: el archivo bajado es idéntico al original, byte a byte.
- El inspector, dentro de la instancia, muestra el bloque con sus tres réplicas `ok`.
- Con `dn2` y el ControlNode líder apagados, el cliente de fuera siguió leyendo y escribiendo.
- Desde fuera responden el 22, los tres puertos de ControlNode y los tres de DataNode. El 6000 de Raft no responde.
- `secrets/` quedó con dueño `1000:1000`, el usuario de la imagen, y la llave de la CA con modo `600`. En macOS esto no se podía comprobar.
- El reloj de la instancia estaba sincronizado por NTP. Importa porque las capabilities vencen según el reloj del DataNode.

## Apagar y borrar

```bash
aws ec2 stop-instances --instance-ids <id>                  # conserva el disco
aws ec2 terminate-instances --instance-ids <id>             # borra la instancia y sus datos
aws ec2 delete-security-group --group-name dfsha-cluster    # después de terminar la instancia
aws ec2 delete-key-pair --key-name dfsha-key
```

## Límites

- **Una sola máquina.** Los seis contenedores toleran que caiga un nodo, no que caiga la instancia.
- **La IP pública cambia** cada vez que la instancia se apaga y se enciende. Una Elastic IP la dejaría fija; no se usó.
- **Sin reinicio automático.** Los contenedores no tienen política de reinicio: después de encender la instancia hay que correr `up -d`.
- **Tres DataNodes.** `docker-compose.publico.yml` no se combina con `.env.escala`.
