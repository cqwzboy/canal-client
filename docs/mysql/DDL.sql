create table canal_client_error_log
(
  `id` int auto_increment primary key not null comment '主键',
  `type` int not null comment '错误类型，1-kafka消费过程中产生的异常， 2-canal客户端监听过程中产生的异常',
  `biz_json` longtext default null comment '业务数据',
  `stack_error` varchar(4000) default null comment '错误堆栈信息',
  `create_time` timestamp default current_timestamp comment '创建时间'
);