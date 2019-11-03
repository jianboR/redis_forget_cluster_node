# redis_forget_cluster_node
compile：gcc -o redis_forget_cluster_node -lpthread -lhiredis redis_forget_node.c

usage: ./redis_forget_cluster_node redis_ip redis_port node_name [pwd]
