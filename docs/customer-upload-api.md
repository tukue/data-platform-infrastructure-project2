# Customer Upload API

The platform provides authenticated endpoints for customers to upload CSV files
to the private raw uploads bucket using resumable multipart uploads.

## What Deploys Automatically

- A Cognito user pool with self-service email sign-up and verification.
- A Cognito app client with no client secret.
- A regional API Gateway endpoint protected by the user pool.
- A Lambda function that starts multipart uploads, provides short-lived part
  URLs, and completes validated uploads up to 5 GiB.

No identity provider, API key, bucket permission, or deployment-time context is
required. Use the `CustomerUploadApiUrl`, `CustomerUploadUserPoolId`, and
`CustomerUploadClientId` CloudFormation outputs to connect a customer client.

## Customer Flow

```text
Customer signs up and verifies email in Cognito
  -> customer starts an upload with fileName and fileSize
  -> API returns uploadId, objectKey, part size, and initial part URLs
  -> customer uploads parts and records the ETag from each response
  -> customer refreshes expired or retry URLs when needed
  -> customer completes the multipart upload through the API
  -> S3 event starts the existing processing workflow
```

The API assigns each customer upload to a unique customer-specific prefix. The
raw bucket stays private; customers receive only expiring URLs for their own
prefix and cannot list, read, or overwrite arbitrary objects.

## Start an Upload

```http
POST /uploads
Authorization: Bearer <cognito-access-token>
Content-Type: application/json

{"fileName":"customers.csv","fileSize":52428800}
```

The response contains `objectKey`, `uploadId`, `partSizeBytes`, `partCount`,
and initial `partUrls`. Upload each part with a `PUT` request and retain the
`ETag` response header.

## Continue and Complete

Use `POST /uploads/{uploadId}/parts` with `objectKey` and a batch of
`partNumbers` to fetch or refresh up to 20 part URLs. Complete the upload with
`POST /uploads/{uploadId}/complete`, passing `objectKey` and a `parts` array of
part numbers and ETags. Do not alter the returned object key or upload ID.
