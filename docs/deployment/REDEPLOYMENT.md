# Redeployment to Existing AWS ECS Service

This GitHub Actions workflow is configured to redeploy the AI TA Backend to your existing AWS ECS Fargate service.

## 🎯 What This Workflow Does

1. **Builds** a new Docker image from your code
2. **Pushes** the image to your existing ECR repository
3. **Downloads** the current task definition from ECS
4. **Updates** the task definition with the new image
5. **Redeploys** the service with zero downtime

## ⚙️ Configuration Required

### 1. Environment Variables

Your application uses the existing `.env.template` file as a reference for required environment variables. For ECS deployment, these should be configured either:

- **As environment variables** in your ECS task definition
- **As secrets** in AWS Secrets Manager (recommended for sensitive values)

Copy `.env.template` to `.env` and fill in your production values for local testing.

### 2. Update GitHub Actions Environment Variables

Edit `.github/workflows/deploy-to-ecs.yml` and update these values to match your existing AWS resources:

```yaml
env:
  AWS_REGION: us-east-1                  # Your AWS region
  ECR_REPOSITORY: ai-ta-backend         # Your ECR repository name
  ECS_SERVICE: ai-ta-backend-service    # Your ECS service name
  ECS_CLUSTER: ai-ta-backend-cluster    # Your ECS cluster name
  ECS_TASK_DEFINITION: ai-ta-backend-task-definition # Your task definition family name
  CONTAINER_NAME: ai-ta-backend         # Container name in your task definition
```

### 3. GitHub Secrets

Add these secrets to your GitHub repository (Settings → Secrets and variables → Actions):

- `AWS_ACCESS_KEY_ID` - AWS access key with ECS permissions
- `AWS_SECRET_ACCESS_KEY` - AWS secret key

### 4. Required AWS IAM Permissions

The AWS user/role needs these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecr:GetAuthorizationToken",
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "ecr:InitiateLayerUpload",
                "ecr:UploadLayerPart",
                "ecr:CompleteLayerUpload",
                "ecr:PutImage"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ecs:UpdateService",
                "ecs:DescribeServices",
                "ecs:DescribeTaskDefinition",
                "ecs:RegisterTaskDefinition"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                "arn:aws:iam::YOUR_ACCOUNT_ID:role/ecsTaskExecutionRole",
                "arn:aws:iam::YOUR_ACCOUNT_ID:role/ecsTaskRole"
            ]
        }
    ]
}
```

## 🚀 How to Deploy

### Automatic Deployment
Push code to `main` or `illinois-chat` branch:
```bash
git add .
git commit -m "Your changes"
git push origin illinois-chat
```

### Manual Deployment
1. Go to your GitHub repository
2. Click "Actions" tab
3. Select "Redeploy to AWS ECS Fargate"
4. Click "Run workflow"
5. Choose the branch and click "Run workflow"

## 📊 Monitoring Deployment

1. **GitHub Actions**: Watch the workflow progress in the Actions tab
2. **ECS Console**: Monitor service updates in AWS ECS console
3. **CloudWatch Logs**: Check `/ecs/ai-ta-backend` log group for application logs

## 🔍 Deployment Process

The workflow uses a **rolling deployment** strategy:

1. New tasks are started with the updated image
2. Health checks ensure new tasks are healthy
3. Old tasks are gracefully stopped
4. Traffic is automatically routed to new tasks

**Expected deployment time**: 2-5 minutes

## ⚡ Key Features

- **Zero Downtime**: Rolling deployment keeps service available
- **Health Checks**: Uses `/health` endpoint to verify deployment success
- **Rollback Ready**: Previous task definition remains available for quick rollback
- **Image Tagging**: Each deployment tagged with git commit SHA
- **Latest Tag**: Also maintains a `latest` tag for convenience

## 🛠️ Troubleshooting

### Common Issues

1. **Workflow fails at "Login to Amazon ECR"**
   - Check AWS credentials in GitHub secrets
   - Verify IAM permissions for ECR

2. **Deployment takes too long**
   - Check ECS service health check configuration
   - Verify application starts quickly and responds to `/health`

3. **New tasks fail health checks**
   - Check CloudWatch logs for application errors
   - Verify `/health` endpoint is accessible on port 8001

4. **Permission denied errors**
   - Ensure IAM user has required ECS and ECR permissions
   - Check task execution role has necessary permissions

### Manual Rollback

If needed, you can quickly rollback to the previous version:

```bash
# Get previous task definition revision
aws ecs describe-services --cluster YOUR_CLUSTER --services YOUR_SERVICE

# Update service to use previous task definition
aws ecs update-service \
  --cluster YOUR_CLUSTER \
  --service YOUR_SERVICE \
  --task-definition YOUR_TASK_DEFINITION:PREVIOUS_REVISION
```

## 📝 Logs and Monitoring

- **Application Logs**: CloudWatch → `/ecs/ai-ta-backend`
- **Service Events**: ECS Console → Cluster → Service → Events tab
- **Deployment Status**: GitHub Actions workflow logs
- **Health Status**: ECS Console → Service → Health and metrics

The deployment is complete when:
- ✅ GitHub Actions workflow shows "success"
- ✅ ECS service shows "steady state"
- ✅ New tasks pass health checks
- ✅ `/health` endpoint returns healthy status
