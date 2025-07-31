from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    Size,
    CfnOutput,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as eventsources,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_sqs as sqs,
    aws_ecr_assets as ecrassets,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_scheduler as scheduler,
    aws_scheduler_targets as targets
)
from constructs import Construct

# User information (NEED TO UPDATE)
region = 'ap-southeast-1'
account = 'XXXXX'
email = 'XXXXX' # Email notification if VT quota exceeded and Lambda failed to disable (will cause looping function)
vt_group_id = 'XXXXX'
vt_api_key = 'XXXXX'

# VPC information (NEED TO UPDATE)
subnet_ids = ['XXXXX']
sg_id = 'XXXXX'

class EnricherCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        s3_bucket = s3.Bucket(
            self, "s3",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            # bucket_name=s3_id,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        CfnOutput(self, "s3_bucket_name", value=s3_bucket.bucket_name)

        db_table = dynamodb.Table(
            self, "db",
            partition_key=dynamodb.Attribute(
                name="UploadFileName",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="TimeStamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            # table_name=db_id,
            removal_policy=RemovalPolicy.DESTROY
        )
        CfnOutput(self, "db_table_name", value=db_table.table_name)

        sns_topic = sns.Topic(
            self, "sns",
            # topic_name=sns_id
        )
        CfnOutput(self, "sns_topic_name", value=sns_topic.topic_name)

        slugify_lib = _lambda.LayerVersion(
            self, "slugify_lib",
            code=_lambda.Code.from_asset("lib/slugify_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="slugify"
        )

        dns_lib = _lambda.LayerVersion(
            self, "dns_lib",
            code=_lambda.Code.from_asset("lib/dns_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="dns"
        )

        whois_lib = _lambda.LayerVersion(
            self, "whois_lib",
            code=_lambda.Code.from_asset("lib/whois_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="whois"
        )

        ipwhois_lib = _lambda.LayerVersion(
            self, "ipwhois_lib",
            code=_lambda.Code.from_asset("lib/ipwhois_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="ipwhois"
        )

        bs4_lib = _lambda.LayerVersion(
            self, "bs4_lib",
            code=_lambda.Code.from_asset("lib/bs4_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="bs4"
        )

        crtsh_lib = _lambda.LayerVersion(
            self, "crtsh_lib",
            code=_lambda.Code.from_asset("lib/crtsh_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="crtsh"
        )

        waybackpy_lib = _lambda.LayerVersion(
            self, "waybackpy_lib",
            code=_lambda.Code.from_asset("lib/waybackpy_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="waybackpy"
        )

        requests_lib = _lambda.LayerVersion(
            self, "requests_lib",
            code=_lambda.Code.from_asset("lib/requests_lib.zip"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            layer_version_name="requests"
        )

        sqs_ss = sqs.Queue(
            self, "sqs_ss",
            visibility_timeout=Duration.minutes(5), # 10 messages per batch, 20s timeout -> 10 * 20 = 200s at least
            retention_period=Duration.days(4),
            # queue_name=sqs_ss_id
        )
        CfnOutput(self, "sqs_ss_name", value=sqs_ss.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_ss,
                filter_policy={
                    "ss_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        ecr_image = ecrassets.DockerImageAsset(
            self, "ecr_image",
            directory="lib/docker",
            platform=ecrassets.Platform.LINUX_AMD64,
            # build_args={ # Does not support dynamic variables
            #     "ACCOUNT": account,
            #     "QUEUE_NAME": sqs_ss.queue_name,
            #     "S3_ID": s3_bucket.bucket_name,
            #     "DB_ID": db_table.table_name,
            #     "QUEUE_URL": sqs_ss.queue_url
            # }
        ) # Creates a default ECR repository.
        CfnOutput(self, "ecr_image_uri", value=ecr_image.image_uri)

        ecs_td = ecs.TaskDefinition(
            self, "ecs_td",
            compatibility=ecs.Compatibility.FARGATE,
            cpu="1024",
            memory_mib="4096"
        )
        CfnOutput(self, "ecs_td_name", value=ecs_td.family)

        ecs_container = ecs_td.add_container(
            "ecs_container",
            image=ecs.ContainerImage.from_docker_image_asset(ecr_image),
            environment={
                "ACCOUNT": account,
                "QUEUE_NAME": sqs_ss.queue_name,
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
                "QUEUE_URL": sqs_ss.queue_url
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="ecs"
            )
        )
        CfnOutput(self, "ecs_container_name", value=ecs_container.container_name)

        ecs_vpc= ec2.Vpc.from_lookup(
            self, "ecs_vpc",
            is_default=True
        )

        ecs_cluster = ecs.Cluster(
            self, "ecs_cluster",
            enable_fargate_capacity_providers=True,
            vpc=ecs_vpc
        )
        CfnOutput(self, "ecs_cluster_name", value=ecs_cluster.cluster_name)

        sqs_ss.grant_consume_messages(ecs_td.task_role)
        s3_bucket.grant_read_write(ecs_td.task_role)
        db_table.grant_read_write_data(ecs_td.task_role)

        lambda_csv = _lambda.Function(
            self, "lambda_csv",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="csv_code.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(15),
            layers=[slugify_lib],
            environment={
                "SUBNET_IDS": ",".join(subnet_ids),
                "SECURITY_GROUP_ID": sg_id,
                "DB_ID": db_table.table_name,
                "ECS_CLUSTER_ID": ecs_cluster.cluster_name,
                "ECS_TASKDEFINITION_ID": ecs_td.family
            },
            # function_name=lambda_csv_id
        )
        CfnOutput(self, "lambda_csv_name", value=lambda_csv.function_name)

        lambda_csv.add_event_source(
            eventsources.S3EventSource(
                s3_bucket,
                events=[s3.EventType.OBJECT_CREATED_PUT],
                filters=[s3.NotificationKeyFilter(prefix="upload/", suffix=".csv")]
            )
        )

        lambda_csv.add_permission(
            "AllowS3Invoke",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            source_arn=s3_bucket.bucket_arn
        )
        lambda_csv.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask", "iam:PassRole"],
                resources=["*"],
            )
        )
        s3_bucket.grant_read(lambda_csv)
        db_table.grant_read_write_data(lambda_csv)

        lambda_sns = _lambda.Function(
            self, "lambda_sns",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="sns.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(5),
            reserved_concurrent_executions=1,
            environment={
                "TOPIC_ARN": sns_topic.topic_arn
            },
            # function_name=lambda_sns_id
        )
        CfnOutput(self, "lambda_sns_name", value=lambda_sns.function_name)

        lambda_sns.add_event_source(
            eventsources.DynamoEventSource(
                db_table,
                batch_size=1000,
                max_batching_window=Duration.minutes(1),
                parallelization_factor=1,
                retry_attempts=3,
                bisect_batch_on_error=True,
                starting_position=_lambda.StartingPosition.LATEST,
                filters=[
                    _lambda.FilterCriteria.filter({
                        "eventName": _lambda.FilterRule.is_equal("INSERT"),
                    })
                ]
            )
        )

        db_table.grant_read_write_data(lambda_sns)
        sns_topic.grant_publish(lambda_sns)

        sqs_vt = sqs.Queue(
            self, "sqs_vt",
            visibility_timeout=Duration.minutes(8),
            retention_period=Duration.days(7),
            # queue_name=sqs_vt_id
        )
        CfnOutput(self, "sqs_vt_name", value=sqs_vt.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_vt,
                filter_policy={
                    "vt_status": sns.SubscriptionFilter.string_filter(allowlist=["0"])
                }
            )
        )

        sns_topic.add_subscription(
            subscriptions.EmailSubscription(
                email,
                filter_policy_with_message_body={
                    "Body": sns.FilterOrPolicy.filter(
                        sns.SubscriptionFilter.string_filter(allowlist=["WARNING: VT quota exceeded and failed to disable Lambda function"])
                    )
                }
            )
        )

        self.lambda_vt_quota = _lambda.Function(
            self, "lambda_vt_quota",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="vt_quota.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(5),
            reserved_concurrent_executions=1
            # function_name=lambda_vt_quota_id
        )
        CfnOutput(self, "lambda_vt_quota_name", value=self.lambda_vt_quota.function_name)

        self.lambda_vt = _lambda.Function(
            self, "lambda_vt",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="vt.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(8),
            reserved_concurrent_executions=50,
            layers=[slugify_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
                "API_KEY": vt_api_key,
                "QUEUE_URL": sqs_vt.queue_url,
                "TOPIC_ARN": sns_topic.topic_arn,
            },
            # function_name=lambda_vt_id
        )
        CfnOutput(self, "lambda_vt_name", value=self.lambda_vt.function_name)

        self.lambda_vt_trigger = _lambda.EventSourceMapping(
            self, "lambda_vt_trigger",
            target=self.lambda_vt,
            batch_size=120,
            event_source_arn=sqs_vt.queue_arn,
            max_batching_window=Duration.seconds(10),
            max_concurrency=50
        )
        CfnOutput(self, "lambda_vt_trigger_uri", value=self.lambda_vt_trigger.event_source_mapping_id)

        sqs_vt.grant_consume_messages(self.lambda_vt)
        sqs_vt.grant_send_messages(self.lambda_vt)
        sns_topic.grant_publish(self.lambda_vt)
        s3_bucket.grant_write(self.lambda_vt)
        db_table.grant_read_write_data(self.lambda_vt)
        self.lambda_vt.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:ListEventSourceMappings"
                ],
                resources=["*"]
            )
        )
        self.lambda_vt.add_permission( # Allows VT quota Lambda to invoke VT Lambda
            "AllowLambdaInvoke",
            principal=iam.ServicePrincipal("lambda.amazonaws.com"),
            source_arn=self.lambda_vt_quota.function_arn
        )
        self.lambda_vt_quota.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:UpdateEventSourceMapping"],
                resources=["*"] 
            )
        )
        self.lambda_vt_quota.grant_invoke(self.lambda_vt) # Allows VT Lambda to invoke VT quota Lambda
        
        eventbridge_vt_quota = scheduler.Schedule(
            self, "eventbridge_vt_quota",
            schedule=scheduler.ScheduleExpression.cron(day="*", hour="0", minute="10"),
            target=targets.LambdaInvoke(
                self.lambda_vt_quota,
                input=scheduler.ScheduleTargetInput.from_object(
                    {"eventbridge": "True"}
                ),
                retry_attempts=3
            )
        )
        CfnOutput(self, "eventbridge_vt_quota_name", value=eventbridge_vt_quota.schedule_name)

        eventbridge_vt_quota.apply_removal_policy(RemovalPolicy.DESTROY)

        sqs_dns = sqs.Queue(
            self, "sqs_dns",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(4),
            # queue_name=sqs_dns_id
        )
        CfnOutput(self, "sqs_dns_name", value=sqs_dns.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_dns,
                filter_policy={
                    "dns_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        lambda_dns = _lambda.Function(
            self, "lambda_dns",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="dns_code.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=50,
            layers=[slugify_lib, dns_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
            }
            # function_name=lambda_dns_id
        )
        CfnOutput(self, "lambda_dns_name", value=lambda_dns.function_name)

        lambda_dns.add_event_source(
            eventsources.SqsEventSource(
                sqs_dns,
                batch_size=30,
                max_batching_window=Duration.seconds(10),
                max_concurrency=50
            )
        )

        sqs_dns.grant_consume_messages(lambda_dns)
        s3_bucket.grant_write(lambda_dns)
        db_table.grant_read_write_data(lambda_dns)

        sqs_whois = sqs.Queue(
            self, "sqs_whois",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(4),
            # queue_name=sqs_whois_id
        )
        CfnOutput(self, "sqs_whois_name", value=sqs_whois.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_whois,
                filter_policy={
                    "whois_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        lambda_whois = _lambda.Function(
            self, "lambda_whois",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="whois_code.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=50,
            layers=[slugify_lib, whois_lib, ipwhois_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
            }
        )
        CfnOutput(self, "lambda_whois_name", value=lambda_whois.function_name)


        lambda_whois.add_event_source(
            eventsources.SqsEventSource(
                sqs_whois,
                batch_size=45,
                max_batching_window=Duration.seconds(10),
                max_concurrency=50
            )
        )

        sqs_whois.grant_consume_messages(lambda_whois)
        s3_bucket.grant_write(lambda_whois)
        db_table.grant_read_write_data(lambda_whois)

        sqs_html = sqs.Queue(
            self, "sqs_html",
            visibility_timeout=Duration.minutes(15), # Needs to be more than or equal to Lambda function timeout
            retention_period=Duration.days(4),
            # queue_name=sqs_html_id
        )
        CfnOutput(self, "sqs_html_name", value=sqs_html.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_html,
                filter_policy={
                    "html_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        lambda_html = _lambda.Function(
            self, "lambda_html",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="html_code.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            ephemeral_storage_size=Size.mebibytes(2000),
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=50,
            layers=[slugify_lib, bs4_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
            }
        )
        CfnOutput(self, "lambda_html_name", value=lambda_html.function_name)

        lambda_html.add_event_source(
            eventsources.SqsEventSource(
                sqs_html,
                batch_size=30,
                max_batching_window=Duration.seconds(10),
                max_concurrency=50
            )
        )

        sqs_html.grant_consume_messages(lambda_html)
        s3_bucket.grant_write(lambda_html)
        db_table.grant_read_write_data(lambda_html)

        sqs_cert = sqs.Queue(
            self, "sqs_cert",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(4),
            # queue_name=sqs_cert_id
        )
        CfnOutput(self, "sqs_cert_name", value=sqs_cert.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_cert,
                filter_policy={
                    "cert_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        lambda_cert = _lambda.Function(
            self, "lambda_cert",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="cert.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=50,
            layers=[slugify_lib, crtsh_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
            }
        )
        CfnOutput(self, "lambda_cert_name", value=lambda_cert.function_name)

        lambda_cert.add_event_source(
            eventsources.SqsEventSource(
                sqs_cert,
                batch_size=15,
                max_batching_window=Duration.seconds(10),
                max_concurrency=50
            )
        )

        sqs_cert.grant_consume_messages(lambda_cert)
        s3_bucket.grant_write(lambda_cert)
        db_table.grant_read_write_data(lambda_cert)

        sqs_hist = sqs.Queue(
            self, "sqs_hist",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(4),
            # queue_name=sqs_hist_id
        )
        CfnOutput(self, "sqs_hist_name", value=sqs_hist.queue_name)

        sns_topic.add_subscription(
            subscriptions.SqsSubscription(
                sqs_hist,
                filter_policy={
                    "hist_status": sns.SubscriptionFilter.string_filter(allowlist=["0"]),
                }
            )
        )

        lambda_hist = _lambda.Function(
            self, "lambda_hist",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="hist.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=50,
            layers=[slugify_lib, waybackpy_lib, requests_lib],
            environment={
                "S3_ID": s3_bucket.bucket_name,
                "DB_ID": db_table.table_name,
            }
        )
        CfnOutput(self, "lambda_hist_name", value=lambda_hist.function_name)

        lambda_hist.add_event_source(
            eventsources.SqsEventSource(
                sqs_hist,
                batch_size=8,
                max_batching_window=Duration.seconds(10),
                max_concurrency=50
            )
        )

        sqs_hist.grant_consume_messages(lambda_hist)
        s3_bucket.grant_write(lambda_hist)
        db_table.grant_read_write_data(lambda_hist)