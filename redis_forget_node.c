#include <stdio.h>
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <regex.h>
#include <time.h>

void *thread_routine(void *name);
int param_checker(const char *reg_str,const char *dest);

typedef struct globalVar{
	//forget node_name
	char *node_name;
	//cluster conect passwd
	char *c_pwd;

} globalVar;

globalVar *createGlobalVar(const char *node_name,const char *pwd);
void releaseGlobalVar(globalVar *global_var);
//任务队列结构节点
globalVar *createGlobalVar(const char *node_name,const char *pwd){
	globalVar *g;
	if( (g=(globalVar*)malloc(sizeof(globalVar))) == NULL)
		return NULL;
	if (node_name !=NULL ){
		g->node_name=(char *)malloc(strlen(node_name)+1);
		strcpy(g->node_name,node_name);
	}else{
		g->node_name=NULL;
	}

	if (pwd != NULL){
		g->c_pwd=(char *)malloc(strlen(pwd)+1);
		strcpy(g->c_pwd,pwd);
	}else{
		g->c_pwd=NULL;
	}
	return g;


}
void releaseGlobalVar(globalVar *global_var){
	if(global_var->node_name != NULL){
		free(global_var->node_name);
		global_var->node_name=NULL;

	}
	if(global_var->c_pwd != NULL){
		free(global_var->c_pwd);
		global_var->c_pwd=NULL;


	}
	
	free(global_var);


}
typedef struct clusterNode{
	int (*process)(const char*,int,globalVar*);
	char *ip;
	int port;
	struct clusterNode *prev;
	struct clusterNode *next;
	
} clusterNode;

//保存集群node的链表
typedef struct clusterList{
	clusterNode *head;
	clusterNode *tail;
	unsigned long len;	


} clusterList;

clusterList *listCreate(void){
	clusterList *list;
	if((list = (clusterList*)malloc(sizeof(*list)))==NULL)
		return NULL;
	list->head = list->tail =NULL;
	list->len = 0;
	return list;


}

void listRelease(clusterList *list){
	unsigned long len;
	clusterNode *current,*next;
	current = list->head;
	while(len--){
		next = current->next;
		free(current);
		current=next;


	}
	free(list);


}


clusterList *listAddNodeTail(int (*process)(const char*,int,globalVar*),clusterList *list,char *ip,int port){
	clusterNode *node;
	if((node = (clusterNode*)malloc(sizeof(*node))) == NULL){
		return NULL;
	}
	node->ip = (char *)malloc(strlen(ip)+1);
	strcpy(node->ip,ip);
	node->port = port;
	node->process=process;
	
	if(list->len==0){
		list->head = list->tail = node;
		node->prev = node->next = NULL;

	}
	else{
		node->prev = list->tail;
		node->next = list->tail->next;
		if(list->tail->next == NULL)
		list->tail->next = node;
		list->tail = node;
	}

	list->len++;
	return list;	



}
 

typedef struct
{
	pthread_mutex_t queue_lock;//用来保护clusterList的并发操作
	clusterList *queue_head;
	pthread_t *threadid;
	int max_thread_num;
	int cur_queue_size;
}threadPool;

static threadPool *pool = NULL;
//线程池初始化函数
void pool_init(int (*process)(const char*,int,globalVar*),int max_thread_num, clusterList *c_list,void *name){
	pool = (threadPool *)malloc(sizeof(threadPool));
	pthread_mutex_init(&(pool->queue_lock),NULL);
	pool->queue_head = c_list;
	pool->max_thread_num = max_thread_num;
	pool->cur_queue_size = c_list->len;
	pool->threadid = (pthread_t *)malloc(max_thread_num*sizeof(pthread_t));
	int i = 0;
	for(i = 0; i < max_thread_num; i++){
		pthread_create(&(pool->threadid[i]), NULL, thread_routine,name);
	}
}

void *thread_routine(void *global_var){
	printf("start thread 0x%x\n", pthread_self());
	while(1){
		pthread_mutex_lock(&(pool->queue_lock));
		//线程处理完成
		if( pool->cur_queue_size==0 ){
			pthread_mutex_unlock(&(pool->queue_lock));
			printf("thread 0x%x will exit\n", pthread_self());
			pthread_exit(NULL);
			
		}
		assert(pool->cur_queue_size!=0);
		assert(pool->queue_head->head!=NULL);
		
		pool->cur_queue_size--;
		clusterNode *worker = pool->queue_head->head ;
		pool->queue_head->head = worker->next;
		if (pool->cur_queue_size!=0){
			pool->queue_head->head->prev = worker->prev;
		}
		worker->next=NULL;
		pool->queue_head->len--;
		printf("thread 0x%x:work->ip:%s,work->port:%d\n",pthread_self(),worker->ip,worker->port);
		pthread_mutex_unlock(&(pool->queue_lock));
		(*(worker->process))(worker->ip,worker->port,(globalVar *)global_var);
		free(worker->ip);
		worker->ip=NULL;
		free(worker);
		worker = NULL;
	}
	pthread_exit(NULL);

}
int process(const char *ip,int port,globalVar *global_var){
	int status = 0;
	redisReply* p = NULL;
	redisReply* r = NULL;
	struct timeval tv;
	tv.tv_sec=1;
	tv.tv_usec=10000;
	printf("start process %s:%d\n",ip,port);
	//redisContext* conn = redisConnect(ip, port);
	redisContext* conn = redisConnectWithTimeout(ip, port,tv);
	if(NULL==conn || conn->err){

                if(conn){
                        fprintf(stderr,"Error:ip:port %s:%d->%s\n",ip,port,conn->errstr);
                        redisFree(conn);
			conn=NULL;

                }else{

                        fprintf(stderr,"Error:Can't allocate redis context\n");
                }
                status = -1;
		goto error;
        }
	if (global_var->c_pwd != NULL){
                p = (redisReply*)redisCommand(conn,"auth %s",global_var->c_pwd);
                if (p==NULL || p->type==6){
                        if(p->type==6){
                                fprintf(stderr,"Error:redis auth command failed,string is :%s\n",p->str);

                        }
                        else {
                                fprintf(stderr,"Error:Can't allocate redis reply\n");


                        }
                        status = -2;
			goto error;

                }
        }
	if(global_var->node_name==NULL){
		status = -3;
		goto error;

	}
        r = (redisReply*)redisCommand(conn,"cluster forget %s",global_var->node_name);
        if(r==NULL || r->type==6){
                if(r->type==6){
                        fprintf(stderr,"Error:redis exec command failed,string is:%s\n",r->str);

                }
                else{
                        fprintf(stderr,"Error:Can't allocate redis reply\n");

                }
                status = -4;
		goto error;

        }
error:
	printf("ip:prot->%s:%d have been processed!\n",ip,port);
	if (p != NULL)
		freeReplyObject(p);
	if (r != NULL)
		 freeReplyObject(r);
        if (NULL!=conn){
                redisFree(conn);
        }
	return status;
}

int param_checker(const char *reg_str,const char *dest){

	regex_t reg;
	char ebuff[256];
	int ret;
	int cflags;
	int status=0;
	cflags=REG_EXTENDED|REG_NEWLINE;
	if(regcomp(&reg, reg_str, cflags)!=0){
		regerror(ret, &reg, ebuff, 256);
		fprintf(stderr, "Error:%s\n", ebuff);
		status= -1;
		goto error;

	}
	if( (ret=regexec(&reg,dest,0,NULL,0)) != 0){
		regerror(ret,&reg,ebuff,256);
		fprintf(stderr, "Error:%s\n", ebuff);
		status = -1;
		goto error;

	}

error:
	regfree(&reg);

	return status;


}

//获取集群的节点
int get_cluster_nodes(clusterList *c_list,const char *ip,int port,const char *pwd){
    	const char *reg_str = "^((([0-9]{1,2})|(1[0-9]{1,2})|(2[0-4][0-9])|(25[0-5]))\\.){3}(([0-9]{1,2})|(1[0-9]{1,2})|(2[0-4][0-9])|(25[0-5]))$";
	const char *reg_port = "^[1-9][0-9]{3}$";
	redisContext* conn = redisConnect(ip, port);
	if(NULL==conn || conn->err){

		if(conn){
			fprintf(stderr,"Error:%s\n",conn->errstr);
			redisFree(conn);

		}else{

			fprintf(stderr,"Error:Can't allocate redis context\n");
		}
		return -1;
	}
	if (pwd != NULL){
		redisReply* p = (redisReply*)redisCommand(conn,"auth %s",pwd);
		if (p==NULL || p->type==6){
			if(p->type==6){
				fprintf(stderr,"Error:redis auth command failed,string is :%s\n",p->str);

			}
			else {
				fprintf(stderr,"Error:Can't allocate redis reply\n");


			}
			return -2;

		}
	}
	const char* command = "cluster nodes";
	redisReply* r = (redisReply*)redisCommand(conn, command);
	if(r==NULL || r->type==6){
		if(r->type==6){
			fprintf(stderr,"Error:redis exec command failed,string is :%s\n",r->str);

		}
		else{
			fprintf(stderr,"Error:Can't allocate redis reply\n");

		}
		return -2;

	}
	//printf("%s\n",r->str);
	char *lines = r->str, *p, *line;
	while ((p = strstr(lines, "\n")) != NULL) {
		*p = '\0';
		line = lines;
		lines = p+1;
		char *name = NULL, *addr = NULL, *flags = NULL, *master_id = NULL,
		*ping_sent = NULL, *ping_recv = NULL, *config_epoch = NULL,
		*link_status = NULL,*slots=NULL;
		int i=0;
		while ((p = strchr(line, ' ')) != NULL) {
			*p = '\0';
			char *token = line;
			line = p + 1;
			switch(i++){
			case 0: name = token; break;
			case 1: addr = token; break;
			}
			if(i == 3) break;


		}
		if (addr == NULL) {

			fprintf(stderr,"Warning:%s get %s unexpected and will not add to clusterlist\n",name,addr);

		}
		char *cip = addr;
		char *c = strrchr(addr, '@');
		if (c != NULL) *c = '\0';
		c = strrchr(addr, ':');
		if (c == NULL) {
			fprintf(stderr,"Warning:%s get %s unexpected and will not add to clusterlist\n",name,ip);
		}
		*c='\0';
		
		int port =  atoi(++c);

		printf("%s:%d\n",cip,port);
		if( param_checker(reg_str,cip) == 0 && param_checker(reg_port,c) == 0){
			if(listAddNodeTail(process,c_list,cip,port) == NULL ){
				fprintf(stderr,"Error:node_name %s,ip:port %s:%d add list failed\n",name,cip,port);
	
			
				return -3;

			}
		}else{
			fprintf(stderr,"Error:node_name %s add list failed\n",name);

		}
		
		
	}	
	if(r!=NULL) freeReplyObject(r);
	if (NULL!=conn){
		redisFree(conn);
	}
	return 0;

}
int main(int argc,char **argv){
	if (argc < 4 || argc > 5 ){
		
		fprintf(stderr,"Usage:cmd ip port forget_node_name [pwd]\n");
		return -1;
	
	}
	const char *gcn_ip;
	int gcn_port;
	const char *gcn_node_name;
	char *gcn_pwd = NULL;

	const char *reg_ip="^((([0-9]{1,2})|(1[0-9]{1,2})|(2[0-4][0-9])|(25[0-5]))\\.){3}(([0-9]{1,2})|(1[0-9]{1,2})|(2[0-4][0-9])|(25[0-5]))$";
	const char *reg_port="^[1-9][0-9]{3}$";
	const char *node_name="^[a-zA-Z0-9]{40}$";
	if(param_checker(reg_ip,argv[1]) !=0 ){
		fprintf(stderr,"输入IP格式不正确\n");
		return -1;

	}
	if(param_checker(reg_port,argv[2]) !=0 ){
		fprintf(stderr,"输入PORT格式不正确\n");
		return -1;

	}
	if(param_checker(node_name,argv[3]) !=0 ){
		fprintf(stderr,"输入forget的node_name格式不正确,应该为40位数字字母组成的字符串\n");
		return -1;

	}
	gcn_ip=argv[1];
	gcn_port=atoi(argv[2]);
	gcn_node_name=argv[3];
	if(argc == 5){
		gcn_pwd=argv[4];
	}

	clusterList *c_list;
	c_list = listCreate();
	globalVar *global_var;
	global_var =createGlobalVar(gcn_node_name,gcn_pwd);
	if(get_cluster_nodes(c_list,gcn_ip,gcn_port,gcn_pwd)<0){
		printf("获取集群节点信息失败\n");
		return -1;

	}
	pool_init(process,3,c_list,global_var);
	int i;
	for(i = 0; i< pool->max_thread_num; i++)
        	pthread_join(pool->threadid[i], NULL);
	free(pool->threadid);
	pthread_mutex_destroy(&(pool->queue_lock));
	free(pool);
	releaseGlobalVar(global_var);
	global_var=NULL;
}
