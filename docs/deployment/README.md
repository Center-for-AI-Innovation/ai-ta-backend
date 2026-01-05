# AWS ECS Fargate Deployment with GitHub Actions

This repository includes a complete CI/CD pipeline for deploying the AI TA Backend to AWS ECS Fargate using GitHub Actions.

## 🚀 Quick Start

1. **Run the setup script** (one-time setup):
   ```bash
   ./scripts/setup-ecs-deployment.sh
   ```

2. **Configure GitHub Secrets**:
   - Go to your GitHub repository → Settings → Secrets and variables → Actions
   - Add these secrets:
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`

3. **Push to main or illinois-chat branch** to trigger deployment

## 📁 Files Added

- `.github/workflows/deploy-to-ecs.yml` - GitHub Actions workflow
- `ai-ta-backend-task-definition.json` - ECS task definition
- `Dockerfile.ecs` - Optimized Docker file for ECS
- `scripts/setup-ecs-deployment.sh` - Automated setup script
- `docs/deployment/aws-ecs-setup.md` - Detailed setup guide
- `.env.ecs.example` - Production environment variables template

## 🔧 Customization

### Environment Variables
Edit the workflow file to customize:
- AWS region
- ECR repository name
- ECS cluster and service names
- CPU/memory allocation

### Task Definition
Update `ai-ta-backend-task-definition.json` to:
- Add environment variables
- Configure secrets from AWS Secrets Manager
- Adjust resource allocation
- Add additional containers

### Health Check
The application now includes a `/health` endpoint that returns:
```json
{
  "status": "healthy",
  "service": "ai-ta-backend",
  "timestamp": 1643723400.123
}
```

## 🏗️ Architecture

```
GitHub → GitHub Actions → AWS ECR → AWS ECS Fargate
```

1. **GitHub Actions** builds and pushes Docker image to ECR
2. **ECR** stores the container image
3. **ECS Fargate** runs the containerized application
4. **CloudWatch** collects logs and metrics

## 🔍 Monitoring

- **Logs**: Available in CloudWatch under `/ecs/ai-ta-backend`
- **Metrics**: ECS service metrics in CloudWatch
- **Health**: HTTP health check on `/health` endpoint

## 🛠️ Troubleshooting

### Common Issues

1. **Service won't start**:
   - Check CloudWatch logs
   - Verify task definition configuration
   - Ensure ECR image exists

2. **Image pull errors**:
   - Verify IAM permissions for ECR
   - Check if repository exists
   - Ensure image was pushed successfully

3. **Network connectivity**:
   - Check security group rules
   - Verify subnet configuration
   - Ensure public IP assignment

### Useful Commands

```bash
# Check service status
aws ecs describe-services --cluster ai-ta-backend-cluster --services ai-ta-backend-service

# View recent logs
aws logs tail /ecs/ai-ta-backend --follow

# Force new deployment
aws ecs update-service --cluster ai-ta-backend-cluster --service ai-ta-backend-service --force-new-deployment
```

## 💰 Cost Optimization

- Use Fargate Spot for non-production environments
- Configure auto-scaling based on metrics
- Consider using smaller instance sizes
- Set up CloudWatch alarms for cost monitoring

## 🔐 Security Best Practices

- Store sensitive values in AWS Secrets Manager
- Use IAM roles with minimal permissions
- Enable CloudTrail for audit logging
- Configure VPC with private subnets for production

## 📚 Additional Resources

- [AWS ECS Documentation](https://docs.aws.amazon.com/ecs/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Detailed Setup Guide](docs/deployment/aws-ecs-setup.md)
