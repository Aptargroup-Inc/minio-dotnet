using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.DataModel.Encryption;
using Minio.DataModel.ILM;
using Minio.DataModel.Notification;
using Minio.DataModel.ObjectLock;
using Minio.DataModel.Replication;
using Minio.DataModel.Response;
using Minio.DataModel.Result;
using Minio.DataModel.Select;
using Minio.DataModel.Tags;
using Minio.Exceptions;
using Minio.Handlers;

namespace Minio;

public class AzureBlobClient : IMinioClient
{
    private readonly string connectionString;
    private readonly BlobServiceClient blobServiceClient;
    private const string MetaElementPrefix = "X-Amz-Meta-";

    public MinioConfig Config => throw new NotSupportedException();

    public IEnumerable<IApiResponseErrorHandler> ResponseErrorHandlers => throw new NotSupportedException();

    public IApiResponseErrorHandler DefaultErrorHandler => throw new NotSupportedException();

    public IRequestLogger RequestLogger => throw new NotSupportedException();

    public AzureBlobClient(string _connectionString)
    {
        connectionString = _connectionString;
        blobServiceClient = new BlobServiceClient(connectionString);
    }


    public async Task<bool> BucketExistsAsync(BucketExistsArgs args,
        CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);

        return await container.ExistsAsync(cancellationToken).ConfigureAwait(false);
    }


    public async IAsyncEnumerable<Item> ListObjectsEnumAsync(ListObjectsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        if (!await BucketExistsAsync(new BucketExistsArgs().WithBucket(args.BucketName), cancellationToken)
                .ConfigureAwait(false))
            throw new BucketNotFoundException(args.BucketName, $"Bucket \"{args.BucketName}\" is not found");

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);

        if (args.Recursive)
        {
            // Flat listing (like MinIO recursive = true)
            var blobList = containerClient.GetBlobsAsync(prefix: args.Prefix, cancellationToken: cancellationToken).ConfigureAwait(false);

            var orderedVersions = new List<BlobItem>();
            await foreach (var version in blobList)
                orderedVersions.Add(version);

            // S3 lists objects in lexicographic ascending order
            var sorted = orderedVersions.OrderBy(v => v.Name, StringComparer.Ordinal);

            foreach (var blobItem in sorted)
            {
                yield return new Item
                {
                    Key = blobItem.Name,
                    LastModified = blobItem.Properties.LastModified?.UtcDateTime.ToString(CultureInfo.InvariantCulture) ?? default,
                    ETag = blobItem.Properties.ETag?.ToString(),
                    Size = (ulong)(blobItem.Properties.ContentLength ?? 0),
                    VersionId = blobItem.VersionId,
                    ContentType = blobItem.Properties.ContentType,
                    Expires = blobItem.Properties.ExpiresOn?.UtcDateTime.ToString(CultureInfo.InvariantCulture) ?? default,
                    UserMetadata = blobItem.Metadata,
                    IsDir = false,
                    IsLatest = blobItem.IsLatestVersion == true
                };
            }
        }
        else
        {
            // Hierarchical listing (like MinIO recursive = false, delimiter = "/")
            var blobList = containerClient.GetBlobsByHierarchyAsync( delimiter: "/", prefix: args.Prefix, cancellationToken: cancellationToken).ConfigureAwait(false);

            var orderedVersions = new List<BlobHierarchyItem>();
            await foreach (var version in blobList)
                orderedVersions.Add(version);

            // S3 lists objects in lexicographic ascending order
            var sorted = orderedVersions.OrderBy(v => v.IsPrefix ? v.Prefix : v.Blob?.Name ?? string.Empty, StringComparer.Ordinal);

            foreach (var item in sorted)
            {
                if (item.IsPrefix)
                {
                    yield return new Item
                    {
                        Key = item.Prefix,
                        IsDir = true
                    };
                }
                else
                {
                    yield return new Item
                    {
                        Key = item.Blob.Name,
                        LastModified = item.Blob.Properties.LastModified?.UtcDateTime.ToString(CultureInfo.InvariantCulture) ?? default,
                        ETag = item.Blob.Properties.ETag?.ToString(),
                        Size = (ulong)(item.Blob.Properties.ContentLength ?? 0),
                        VersionId = item.Blob.VersionId,
                        ContentType = item.Blob.Properties.ContentType,
                        Expires = item.Blob.Properties.ExpiresOn?.UtcDateTime.ToString(CultureInfo.InvariantCulture) ?? default,
                        UserMetadata = item.Blob.Metadata,
                        IsDir = false,
                        IsLatest = item.Blob.IsLatestVersion == true
                    };
                }
            }
        }
    }


    public Task<string> PresignedGetObjectAsync(PresignedGetObjectArgs args)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        // S3 object keys should not start with '/', but handle it for compatibility
        var objectName = args.ObjectName.StartsWith("/", StringComparison.Ordinal) ? args.ObjectName[1..] : args.ObjectName;
        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(objectName);

        try
        {
            // Try to generate a new SAS URI (requires StorageSharedKeyCredential)
            var sasUri = blobClient.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddSeconds(args.Expiry));
            return Task.FromResult(sasUri.ToString());
        }
        catch (ArgumentNullException ex) when (string.Equals(ex.ParamName, "sharedKeyCredential", StringComparison.Ordinal))
        {
            // If authenticated with SAS token (not account key), we cannot generate new SAS tokens
            // Workaround: Return the blob URL with the existing SAS token from connection string
            // Note: The expiry will be based on the original SAS token, not the requested expiry
            
            // Extract SAS token from connection string
            var sasToken = ExtractSasTokenFromConnectionString();
            if (string.IsNullOrEmpty(sasToken))
            {
                throw new NotSupportedException(
                    "Cannot generate presigned URLs when authenticated with SAS token. " +
                    "To generate presigned URLs, use account key authentication instead of SAS token. " +
                    "Alternatively, use the existing SAS token URL returned by this method (with original expiry).",
                    ex);
            }

            // Return URL with existing SAS token
            var blobUrl = blobClient.Uri.AbsoluteUri;
            var urlWithSas = sasToken.StartsWith("?", StringComparison.Ordinal) 
                ? $"{blobUrl}{sasToken}" 
                : $"{blobUrl}?{sasToken}";
            
            return Task.FromResult(urlWithSas);
        }
    }

    private string ExtractSasTokenFromConnectionString()
    {
        // Parse connection string to extract SharedAccessSignature
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        foreach (var part in parts)
        {
            if (part.StartsWith("SharedAccessSignature=", StringComparison.OrdinalIgnoreCase))
            {
                return part["SharedAccessSignature=".Length..];
            }
        }
        return null;
    }


    public async Task<PutObjectResponse> PutObjectAsync(PutObjectArgs args,
        CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);

        // S3 Standard: PutObject should fail with NoSuchBucket if bucket doesn't exist
        // Unlike MakeBucket, PutObject must NOT auto-create the bucket
        if (!await container.ExistsAsync(cancellationToken).ConfigureAwait(false))
            throw new BucketNotFoundException(args.BucketName, $"Bucket \"{args.BucketName}\" does not exist");

        var blobClient = container.GetBlobClient(args.ObjectName);

        var userMetaData = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var xHeader in args.Headers.Where(x =>
                     x.Key.StartsWith(MetaElementPrefix,
                         StringComparison.OrdinalIgnoreCase)))
        {
            var key = xHeader.Key[MetaElementPrefix.Length..];
            userMetaData[key] = xHeader.Value;
        }
        var response = await blobClient.UploadAsync(content: args.ObjectStreamData, new BlobHttpHeaders { ContentType = args.ContentType }, metadata: userMetaData).ConfigureAwait(false);

        return new PutObjectResponse(
            (HttpStatusCode)response.GetRawResponse().Status,
            Encoding.Default.GetString(response.GetRawResponse().Content),
            response.GetRawResponse().Headers.ToDictionary(h => h.Name, h => h.Value, StringComparer.OrdinalIgnoreCase),
            args.ObjectSize,
            args.ObjectName
            );
    }


    public async Task RemoveObjectAsync(RemoveObjectArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(args.ObjectName);

        if (!string.IsNullOrEmpty(args.VersionId))
        {
            // To remove a version that is the promoted one, another version must be promoted otherwise it will throw 403 error
            var latestBlobItem = GetDifferentLatestVersionIfExists(args.BucketName, args.ObjectName, args.VersionId);
            if (latestBlobItem != null)
            {
                await PromoteLatestVersionAsync(latestBlobItem.VersionId, blobClient).ConfigureAwait(false);
                blobClient = blobClient.WithVersion(args.VersionId);
            }
        }

        _ = await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }


    public async Task<ObjectStat> StatObjectAsync(StatObjectArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        if (!await BucketExistsAsync(new BucketExistsArgs().WithBucket(args.BucketName), cancellationToken)
                .ConfigureAwait(false))
            throw new BucketNotFoundException(args.BucketName, $"Bucket \"{args.BucketName}\" is not found");

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(args.ObjectName);

        if (!await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
            return new StatObjectResponse(HttpStatusCode.NotFound, null, new Dictionary<string, string>(StringComparer.Ordinal), args).ObjectInfo;

        BlobProperties properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var responseHeaders = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                { "content-length", properties.ContentLength.ToString(CultureInfo.InvariantCulture) },
                { "etag", properties.ETag.ToString() },
                { "last-modified", properties.LastModified.UtcDateTime.ToString(CultureInfo.InvariantCulture) },
                { "content-type", properties.ContentType },
                { "x-amz-version-id", properties.VersionId }
            };

        foreach (var metaData in properties.Metadata)
            responseHeaders.Add($"{MetaElementPrefix}{metaData.Key}", metaData.Value);

        var statResponse = new StatObjectResponse(HttpStatusCode.OK, null, responseHeaders, args);

        return statResponse.ObjectInfo;
    }


    private BlobItem GetDifferentLatestVersionIfExists(string bucketName, string objectName, string versionToDelete)
    {
        var containerClient = blobServiceClient.GetBlobContainerClient(bucketName);

        // List all versions of the blob
        var versions = containerClient
            .GetBlobs(BlobTraits.None, BlobStates.Version, prefix: objectName);

        BlobItem latestVersion = null;

        foreach (var v in versions)
        {
            // Skip the one we want to delete
            if (string.Equals(v.VersionId, versionToDelete, StringComparison.Ordinal))
                continue;

            // Pick the most recent version by LastModified
            if (latestVersion == null || v.Properties.LastModified > latestVersion.Properties.LastModified)
            {
                latestVersion = v;
            }
        }

        return latestVersion;
    }


    private async Task PromoteLatestVersionAsync(string latestVersion, BlobClient blobClient)
    {
        var blobClientForPromotion = blobClient;
        var versionClient = blobClientForPromotion.WithVersion(latestVersion);

        // Promote the latest version (overwrite the current blob with it)
        _ = await blobClientForPromotion.StartCopyFromUriAsync(versionClient.Uri).ConfigureAwait(false);
        // Delete the version which has been promoted otherwise it will show two identical versions
        _ = await versionClient.DeleteAsync().ConfigureAwait(false);
    }

    public void SetTraceOff()
    {
        throw new NotSupportedException();
    }

    public void SetTraceOn(IRequestLogger logger = null)
    {
        // 
    }

    public Task<HttpResponseMessage> WrapperGetAsync(Uri uri)
    {
        throw new NotSupportedException();
    }

    public Task WrapperPutAsync(Uri uri, StreamContent strm)
    {
        throw new NotSupportedException();
    }

    public Task MakeBucketAsync(MakeBucketArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        // Create a new container in Azure Blob Storage
        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);
        return container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
    }

    public Task<ListAllMyBucketsResult> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        //list all containers in the Azure Blob Storage
        var containers = blobServiceClient.GetBlobContainers();
        var result = new ListAllMyBucketsResult
        {
            Buckets = new System.Collections.ObjectModel.Collection<Bucket>(
                containers.Select(c => new Bucket { Name = c.Name }).ToList())
        };
        return Task.FromResult(result);
    }

    public Task RemoveBucketAsync(RemoveBucketArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        // Delete the container in Azure Blob Storage
        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);
        return container.DeleteAsync(cancellationToken: cancellationToken);
    }

    public Task<BucketNotification> GetBucketNotificationsAsync(GetBucketNotificationsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetBucketNotificationsAsync(SetBucketNotificationsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveAllBucketNotificationsAsync(RemoveAllBucketNotificationsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public IObservable<MinioNotificationRaw> ListenBucketNotificationsAsync(ListenBucketNotificationsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public async Task<Tagging> GetBucketTagsAsync(GetBucketTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        // Get the tags for the container in Azure Blob Storage
        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var properties = await container.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        return new Tagging(properties.Value.Metadata, false);
    }

    public Task SetBucketTagsAsync(SetBucketTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);

        // SetBucketTagsArgs stores tags in BucketTags; use its Tags dictionary (bucket-level semantics => isObjects = false)
        var tags = args.BucketTags?.Tags;
        if (tags is null || tags.Count == 0)
            throw new InvalidOperationException("Unable to set empty tags.");

        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in tags)
            metadata[kvp.Key] = kvp.Value;

        return container.SetMetadataAsync(metadata, cancellationToken: cancellationToken);
    }

    public Task RemoveBucketTagsAsync(RemoveBucketTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var container = blobServiceClient.GetBlobContainerClient(args.BucketName);

        // Clear all metadata by setting an empty dictionary
        var emptyMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        return container.SetMetadataAsync(emptyMetadata, cancellationToken: cancellationToken);
    }

    public Task SetObjectLockConfigurationAsync(SetObjectLockConfigurationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<ObjectLockConfiguration> GetObjectLockConfigurationAsync(GetObjectLockConfigurationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveObjectLockConfigurationAsync(RemoveObjectLockConfigurationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<VersioningConfiguration> GetVersioningAsync(GetVersioningArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetVersioningAsync(SetVersioningArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetBucketEncryptionAsync(SetBucketEncryptionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<ServerSideEncryptionConfiguration> GetBucketEncryptionAsync(GetBucketEncryptionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveBucketEncryptionAsync(RemoveBucketEncryptionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetBucketLifecycleAsync(SetBucketLifecycleArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<LifecycleConfiguration> GetBucketLifecycleAsync(GetBucketLifecycleArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveBucketLifecycleAsync(RemoveBucketLifecycleArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<ReplicationConfiguration> GetBucketReplicationAsync(GetBucketReplicationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetBucketReplicationAsync(SetBucketReplicationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveBucketReplicationAsync(RemoveBucketReplicationArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<string> GetPolicyAsync(GetPolicyArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public IObservable<MinioNotificationRaw> ListenNotifications(ListenBucketNotificationsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public IObservable<MinioNotificationRaw> ListenBucketNotificationsAsync(string bucketName, IList<EventType> events, string prefix = "", string suffix = "", CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemovePolicyAsync(RemovePolicyArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetPolicyAsync(SetPolicyArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<bool> GetObjectLegalHoldAsync(GetObjectLegalHoldArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetObjectLegalHoldAsync(SetObjectLegalHoldArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task SetObjectRetentionAsync(SetObjectRetentionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<ObjectRetentionConfiguration> GetObjectRetentionAsync(GetObjectRetentionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task ClearObjectRetentionAsync(ClearObjectRetentionArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<IList<DeleteError>> RemoveObjectsAsync(RemoveObjectsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task CopyObjectAsync(CopyObjectArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<ObjectStat> GetObjectAsync(GetObjectArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<SelectResponseStream> SelectObjectContentAsync(SelectObjectContentArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public IAsyncEnumerable<Upload> ListIncompleteUploadsEnumAsync(ListIncompleteUploadsArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task RemoveIncompleteUploadAsync(RemoveIncompleteUploadArgs args, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<(Uri, IDictionary<string, string>)> PresignedPostPolicyAsync(PresignedPostPolicyArgs args)
    {
        throw new NotSupportedException();
    }

    public Task<string> PresignedPutObjectAsync(PresignedPutObjectArgs args)
    {
        throw new NotSupportedException();
    }

    public Task<(Uri, IDictionary<string, string>)> PresignedPostPolicyAsync(PostPolicy policy)
    {
        throw new NotSupportedException();
    }

    public async Task<Tagging> GetObjectTagsAsync(GetObjectTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(args.ObjectName);

        // If a specific version is requested, get that version
        if (!string.IsNullOrEmpty(args.VersionId))
            blobClient = blobClient.WithVersion(args.VersionId);

        var tagsResponse = await blobClient.GetTagsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        // Convert Azure blob tags to Minio Tagging (isObjects = true for object-level tags)
        var tags = tagsResponse.Value.Tags.ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
        return new Tagging(tags, true);
    }

    public Task SetObjectTagsAsync(SetObjectTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(args.ObjectName);

        // If a specific version is specified, set tags on that version
        if (!string.IsNullOrEmpty(args.VersionId))
            blobClient = blobClient.WithVersion(args.VersionId);

        // SetObjectTagsArgs stores tags in ObjectTags; use its Tags dictionary
        var tags = args.ObjectTags?.Tags;
        if (tags is null || tags.Count == 0)
            throw new InvalidOperationException("Unable to set empty tags.");

        // Convert to Azure blob tags format (Dictionary<string, string>)
        var azureTags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kvp in tags)
            azureTags[kvp.Key] = kvp.Value;

        return blobClient.SetTagsAsync(azureTags, cancellationToken: cancellationToken);
    }

    public Task RemoveObjectTagsAsync(RemoveObjectTagsArgs args, CancellationToken cancellationToken = default)
    {
        if (args == null) throw new ArgumentNullException(nameof(args));

        args.Validate();

        var containerClient = blobServiceClient.GetBlobContainerClient(args.BucketName);
        var blobClient = containerClient.GetBlobClient(args.ObjectName);

        // If a specific version is specified, remove tags from that version
        if (!string.IsNullOrEmpty(args.VersionId))
            blobClient = blobClient.WithVersion(args.VersionId);

        // Clear all tags by setting an empty dictionary
        var emptyTags = new Dictionary<string, string>(StringComparer.Ordinal);
        return blobClient.SetTagsAsync(emptyTags, cancellationToken: cancellationToken);
    }

    public void Dispose()
    {
        throw new NotSupportedException();
    }
}
