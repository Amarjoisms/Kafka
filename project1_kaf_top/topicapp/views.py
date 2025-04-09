import paramiko
import json
from django.http import JsonResponse
from sshtunnel import SSHTunnelForwarder
from kafka.admin import KafkaAdminClient, NewTopic
from django.views.decorators.csrf import csrf_exempt



# Configs
SSH_KEY_PATH = "D:/Downloads/u2204.pem"
BASTION_HOST = "54.91.82.230"
KAFKA_PRIVATE_HOST = "10.0.128.234"
USERNAME = "ubuntu"
KAFKA_BIN_PATH = "/opt/kafka_2.13-3.9.0/bin/"
KAFKA_PORT = 9092


def get_kafka_topics(request):
    try:
        # 1. Connect to Bastion
        bastion_key = paramiko.RSAKey.from_private_key_file(SSH_KEY_PATH)
        bastion_client = paramiko.SSHClient()
        bastion_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion_client.connect(hostname=BASTION_HOST, username=USERNAME, pkey=bastion_key)

        # 2. Open tunnel from Bastion to Kafka
        transport = bastion_client.get_transport()
        dest_addr = (KAFKA_PRIVATE_HOST, 22)
        local_addr = ("", 0)
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

        # 3. Connect to Kafka through the tunnel
        kafka_client = paramiko.SSHClient()
        kafka_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kafka_client.connect(
            hostname=KAFKA_PRIVATE_HOST,
            username=USERNAME,
            pkey=bastion_key,
            sock=channel  # Use tunnel
        )

        # 4. Run Kafka topics command
        kafka_cmd = f"{KAFKA_BIN_PATH}/kafka-topics.sh --list --bootstrap-server {KAFKA_PRIVATE_HOST}:9092"
        stdin, stdout, stderr = kafka_client.exec_command(kafka_cmd)

        error_output = stderr.read().decode()
        if error_output:
            print("❌ Kafka error:", error_output)

        topics = stdout.read().decode().split("\n")
        topics = [t.strip() for t in topics if t.strip()]

        # Close connections
        kafka_client.close()
        bastion_client.close()

        return JsonResponse({"topics": topics})

    except Exception as e:
        print(f"❌ Error: {e}")
        return JsonResponse({"error": str(e)}, status=500)
    

@csrf_exempt
def create_topic(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            topic_name = data.get("topic_name")
            num_partitions = int(data.get("num_partitions", 1))
            replication_factor = int(data.get("replication_factor", 1))

            # Create SSH tunnel from localhost -> bastion -> Kafka
            with SSHTunnelForwarder(
                (BASTION_HOST, 22),
                ssh_username=USERNAME,
                ssh_pkey=SSH_KEY_PATH,
                remote_bind_address=(KAFKA_PRIVATE_HOST, KAFKA_PORT),
                local_bind_address=('localhost',)
            ) as tunnel:
                tunnel.start()
                kafka_host = f"localhost:{tunnel.local_bind_port}"

                admin_client = KafkaAdminClient(
                    bootstrap_servers=kafka_host,
                    client_id='django-client'
                )

                topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
                fs = admin_client.create_topics(new_topics=topic_list, validate_only=False)
                for topic, f in fs.items():
                    try:
                        f.result()
                    except Exception as e:
                        print(f"Failed to create topic '{topic}': {e}")

                return JsonResponse({"message": f"Topic '{topic_name}' created successfully"})

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Invalid HTTP method"}, status=405)