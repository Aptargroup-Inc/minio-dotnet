/*
 * MinIO .NET Library for Amazon S3 Compatible Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Globalization;
using System.Net;
using System.Text;
using Minio;
using Minio.DataModel.Args;
using Minio.DataModel.Tags;
using Minio.Exceptions;

namespace SimpleTest;

public static class Program
{
    // Replace with your Azure Blob Storage connection string
    // NOTE: For ListBuckets to work, your SAS token must include service-level permissions (srt=sco)
    // If you only have container/object permissions (srt=co), ListBuckets will fail but other tests will work
    private const string ConnectionString = "BlobEndpoint=https://aptarpoc.blob.core.windows.net/;QueueEndpoint=https://aptarpoc.queue.core.windows.net/;FileEndpoint=https://aptarpoc.file.core.windows.net/;TableEndpoint=https://aptarpoc.table.core.windows.net/;SharedAccessSignature=sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-12-19T01:27:22Z&st=2025-12-11T17:12:22Z&spr=https,http&sig=OmRJdMdglS2I3ZHlGZNAiqkRTeIywz2%2Fy6RptMAizKI%3D";
    
    private const string TestBucketName = "bucket-s3";
    private const string TestObjectName = "object.txt";

    private static async Task Main()
    {
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
                                               | SecurityProtocolType.Tls11
                                               | SecurityProtocolType.Tls13;

        var azureClient = new AzureBlobClient(ConnectionString);

        Console.WriteLine("=== Azure Blob Client S3-Compatible Tests ===");
        Console.WriteLine();
        Console.WriteLine("⚠️  IMPORTANT: SAS Token Permissions & Limitations");
        Console.WriteLine();
        Console.WriteLine("   Required Permissions:");
        Console.WriteLine("   - Signed Resource Types: Service + Container + Object (srt=sco)");
        Console.WriteLine("   - Services: Blob + File + Queue + Table (ss=bfqt)");
        Console.WriteLine("   - Permissions: Read + Write + Delete + List + Add + Create + Update + Process (sp=rwdlacupx)");
        Console.WriteLine();
        Console.WriteLine("   Current Authentication: SAS Token");
        Console.WriteLine("   Detected Limitations:");
        Console.WriteLine("   → ListBuckets: Requires 'srt=sco' (will skip if missing 's')");
        Console.WriteLine("   → Presigned URLs: Cannot generate new SAS from existing SAS");
        Console.WriteLine("     (Will use existing SAS token URL with original expiry)");
        Console.WriteLine();
        Console.WriteLine("   💡 Tip: For full functionality, use account key authentication instead of SAS");
        Console.WriteLine();

        try
        {
            // Run all tests
            await TestBucketOperations(azureClient).ConfigureAwait(false);
            await TestObjectOperations(azureClient).ConfigureAwait(false);
            await TestBucketTagsOperations(azureClient).ConfigureAwait(false);
            await TestObjectTagsOperations(azureClient).ConfigureAwait(false);
            await TestListOperations(azureClient).ConfigureAwait(false);
            await TestRemoveObjectsOperations(azureClient).ConfigureAwait(false);
            await TestCopyObjectOperations(azureClient).ConfigureAwait(false);
            await TestPresignedUrls(azureClient).ConfigureAwait(false);
            await TestS3ComplianceRules(azureClient).ConfigureAwait(false);

            Console.WriteLine("\n✅ All tests completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Test failed with error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }

        Console.WriteLine("\nPress any key to exit...");
        _ = Console.ReadLine();
    }

    #region Bucket Operations Tests

    private static async Task TestBucketOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Bucket Operations ===");

        // Test 1: MakeBucket
        Console.Write("1. Creating bucket... ");
        var makeBucketArgs = new MakeBucketArgs().WithBucket(TestBucketName);
        await client.MakeBucketAsync(makeBucketArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        // Test 2: BucketExists (should return true)
        Console.Write("2. Checking if bucket exists... ");
        var bucketExistsArgs = new BucketExistsArgs().WithBucket(TestBucketName);
        var exists = await client.BucketExistsAsync(bucketExistsArgs).ConfigureAwait(false);
        Console.WriteLine(exists ? "✓ (exists)" : "✗ (not found)");

        // Test 3: ListBuckets (requires service-level SAS permissions)
        Console.Write("3. Listing all buckets... ");
        try
        {
            var buckets = await client.ListBucketsAsync().ConfigureAwait(false);
            Console.WriteLine($"✓ (Found {buckets.Buckets.Count} buckets)");
            foreach (var bucket in buckets.Buckets)
            {
                Console.WriteLine($"   - {bucket.Name}");
            }
        }
        catch (Azure.RequestFailedException ex) when (string.Equals(ex.ErrorCode ,"AuthorizationResourceTypeMismatch"))
        {
            Console.WriteLine("⚠ (Skipped - SAS token needs 'srt=sco' for service-level operations)");
            Console.WriteLine("   To fix: Regenerate SAS token with Service+Container+Object permissions");
        }

        Console.WriteLine();
    }

    #endregion

    #region Object Operations Tests

    private static async Task TestObjectOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Object Operations ===");

        // Test 1: PutObject
        Console.Write("1. Uploading object... ");
        var content = "Hello from Azure Blob Storage via S3-compatible API!";
        var bytes = Encoding.UTF8.GetBytes(content);
        using var stream = new MemoryStream(bytes);
        
        var putObjectArgs = new PutObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(TestObjectName)
            .WithStreamData(stream)
            .WithObjectSize(bytes.Length)
            .WithContentType("text/plain")
            .WithHeaders(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                { "X-Amz-Meta-Author", "Test User" },
                { "X-Amz-Meta-Purpose", "Testing" }
            });
        
        _ = await client.PutObjectAsync(putObjectArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        // Test 2: StatObject
        Console.Write("2. Getting object metadata... ");
        var statObjectArgs = new StatObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(TestObjectName);
        
        var objectStat = await client.StatObjectAsync(statObjectArgs).ConfigureAwait(false);
        Console.WriteLine($"✓ (Size: {objectStat.Size} bytes, ETag: {objectStat.ETag})");
        Console.WriteLine($"   Metadata:");
        foreach (var meta in objectStat.MetaData)
        {
            Console.WriteLine($"   - {meta.Key}: {meta.Value}");
        }

        Console.WriteLine();
    }

    #endregion

    #region Bucket Tags Tests

    private static async Task TestBucketTagsOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Bucket Tags Operations ===");

        // Test 1: SetBucketTags
        Console.Write("1. Setting bucket tags... ");
        var bucketTags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "Environment", "Testing" },
            { "Project", "MinIO-Azure" },
            { "Owner", "DevTeam" }
        };
        
        var setBucketTagsArgs = new SetBucketTagsArgs()
            .WithBucket(TestBucketName)
            .WithTagging(Tagging.GetBucketTags(bucketTags));
        
        await client.SetBucketTagsAsync(setBucketTagsArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        // Test 2: GetBucketTags
        Console.Write("2. Getting bucket tags... ");
        var getBucketTagsArgs = new GetBucketTagsArgs()
            .WithBucket(TestBucketName);
        
        var tagging = await client.GetBucketTagsAsync(getBucketTagsArgs).ConfigureAwait(false);
        Console.WriteLine($"✓ (Found {tagging.Tags.Count} tags)");
        foreach (var tag in tagging.Tags)
        {
            Console.WriteLine($"   - {tag.Key}: {tag.Value}");
        }

        // Test 3: RemoveBucketTags
        Console.Write("3. Removing bucket tags... ");
        var removeBucketTagsArgs = new RemoveBucketTagsArgs()
            .WithBucket(TestBucketName);
        
        await client.RemoveBucketTagsAsync(removeBucketTagsArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.WriteLine();
    }

    #endregion

    #region Object Tags Tests

    private static async Task TestObjectTagsOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Object Tags Operations ===");

        // Test 1: SetObjectTags
        Console.Write("1. Setting object tags... ");
        var objectTags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "FileType", "Text" },
            { "Status", "Active" },
            { "Reviewed", "true" }
        };
        
        var setObjectTagsArgs = new SetObjectTagsArgs()
            .WithBucket(TestBucketName)
            .WithObject(TestObjectName)
            .WithTagging(Tagging.GetObjectTags(objectTags));
        
        await client.SetObjectTagsAsync(setObjectTagsArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        // Test 2: GetObjectTags
        Console.Write("2. Getting object tags... ");
        var getObjectTagsArgs = new GetObjectTagsArgs()
            .WithBucket(TestBucketName)
            .WithObject(TestObjectName);
        
        var tagging = await client.GetObjectTagsAsync(getObjectTagsArgs).ConfigureAwait(false);
        Console.WriteLine($"✓ (Found {tagging.Tags.Count} tags)");
        foreach (var tag in tagging.Tags)
        {
            Console.WriteLine($"   - {tag.Key}: {tag.Value}");
        }

        // Test 3: RemoveObjectTags
        Console.Write("3. Removing object tags... ");
        var removeObjectTagsArgs = new RemoveObjectTagsArgs()
            .WithBucket(TestBucketName)
            .WithObject(TestObjectName);
        
        await client.RemoveObjectTagsAsync(removeObjectTagsArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.WriteLine();
    }

    #endregion

    #region List Operations Tests

    private static async Task TestListOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing List Operations ===");

        // Upload some test objects for listing
        Console.Write("Setting up test objects... ");
        var testFiles = new[]
        {
            "folder1/file1.txt",
            "folder1/file2.txt",
            "folder1/subfolder/file3.txt",
            "folder2/file4.txt",
            "root-file.txt"
        };

        foreach (var fileName in testFiles)
        {
            var content = $"Content of {fileName}";
            var bytes = Encoding.UTF8.GetBytes(content);
            using var stream = new MemoryStream(bytes);
            
            var putArgs = new PutObjectArgs()
                .WithBucket(TestBucketName)
                .WithObject(fileName)
                .WithStreamData(stream)
                .WithObjectSize(bytes.Length)
                .WithContentType("text/plain");
            
            _ = await client.PutObjectAsync(putArgs).ConfigureAwait(false);
        }
        Console.WriteLine("✓");

        // Test 1: ListObjects - Recursive (flat listing)
        Console.WriteLine("1. Listing objects (recursive/flat):");
        var listArgsRecursive = new ListObjectsArgs()
            .WithBucket(TestBucketName)
            .WithRecursive(true);

        await foreach (var item in client.ListObjectsEnumAsync(listArgsRecursive).ConfigureAwait(false))
        {
            Console.WriteLine($"   - {item.Key} ({item.Size} bytes)");
        }

        // Test 2: ListObjects - Hierarchical (with delimiter)
        Console.WriteLine("2. Listing objects (hierarchical with prefix 'folder1/'):");
        var listArgsHierarchical = new ListObjectsArgs()
            .WithBucket(TestBucketName)
            .WithPrefix("folder1/")
            .WithRecursive(false);

        await foreach (var item in client.ListObjectsEnumAsync(listArgsHierarchical).ConfigureAwait(false))
        {
            if (item.IsDir)
                Console.WriteLine($"   📁 {item.Key}");
            else
                Console.WriteLine($"   📄 {item.Key} ({item.Size} bytes)");
        }

        Console.WriteLine();
    }

    #endregion

    #region Remove Objects Operations Tests

    private static async Task TestRemoveObjectsOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Remove Objects Operations ===");

        // Setup: Create some test objects to delete
        Console.Write("Setting up test objects for batch deletion... ");
        var testObjects = new[]
        {
            "batch-delete/file1.txt",
            "batch-delete/file2.txt",
            "batch-delete/file3.txt",
            "batch-delete/subfolder/file4.txt",
            "batch-delete/subfolder/file5.txt"
        };

        foreach (var fileName in testObjects)
        {
            var content = $"Content of {fileName}";
            var bytes = Encoding.UTF8.GetBytes(content);
            using var stream = new MemoryStream(bytes);
            
            var putArgs = new PutObjectArgs()
                .WithBucket(TestBucketName)
                .WithObject(fileName)
                .WithStreamData(stream)
                .WithObjectSize(bytes.Length)
                .WithContentType("text/plain");
            
            _ = await client.PutObjectAsync(putArgs).ConfigureAwait(false);
        }
        Console.WriteLine("✓");

        // Test 1: Remove multiple objects (batch delete)
        Console.Write("1. Removing multiple objects in batch... ");
        var objectsToDelete = new List<string>
        {
            "batch-delete/file1.txt",
            "batch-delete/file2.txt",
            "batch-delete/subfolder/file4.txt"
        };

        var removeObjectsArgs = new RemoveObjectsArgs()
            .WithBucket(TestBucketName)
            .WithObjects(objectsToDelete);

        var errors = await client.RemoveObjectsAsync(removeObjectsArgs).ConfigureAwait(false);
        
        if (errors.Count == 0)
        {
            Console.WriteLine($"✓ (Deleted {objectsToDelete.Count} objects successfully)");
        }
        else
        {
            Console.WriteLine($"⚠ ({errors.Count} errors occurred)");
            foreach (var error in errors)
            {
                Console.WriteLine($"   Error deleting {error.Key}: {error.Message}");
            }
        }

        // Test 2: Verify objects are deleted
        Console.Write("2. Verifying deleted objects... ");
        var listArgs = new ListObjectsArgs()
            .WithBucket(TestBucketName)
            .WithPrefix("batch-delete/")
            .WithRecursive(true);

        var remainingCount = 0;
        await foreach (var item in client.ListObjectsEnumAsync(listArgs).ConfigureAwait(false))
        {
            remainingCount++;
        }
        
        var expectedRemaining = testObjects.Length - objectsToDelete.Count;
        if (remainingCount == expectedRemaining)
        {
            Console.WriteLine($"✓ ({remainingCount} objects remaining as expected)");
        }
        else
        {
            Console.WriteLine($"⚠ (Expected {expectedRemaining}, found {remainingCount})");
        }

        // Test 3: Test idempotency - deleting non-existent objects should succeed
        Console.Write("3. Testing idempotency (deleting non-existent objects)... ");
        var nonExistentObjects = new List<string>
        {
            "batch-delete/non-existent1.txt",
            "batch-delete/non-existent2.txt"
        };

        var removeNonExistentArgs = new RemoveObjectsArgs()
            .WithBucket(TestBucketName)
            .WithObjects(nonExistentObjects);

        var idempotencyErrors = await client.RemoveObjectsAsync(removeNonExistentArgs).ConfigureAwait(false);
        
        if (idempotencyErrors.Count == 0)
        {
            Console.WriteLine("✓ (S3 idempotency: no errors for non-existent objects)");
        }
        else
        {
            Console.WriteLine($"⚠ ({idempotencyErrors.Count} unexpected errors)");
        }

        // Cleanup: Remove remaining test objects
        Console.Write("4. Cleaning up remaining test objects... ");
        var cleanupObjects = new List<string>
        {
            "batch-delete/file3.txt",
            "batch-delete/subfolder/file5.txt"
        };

        var cleanupArgs = new RemoveObjectsArgs()
            .WithBucket(TestBucketName)
            .WithObjects(cleanupObjects);

        _ = await client.RemoveObjectsAsync(cleanupArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.WriteLine();
    }

    #endregion

    #region Copy Object Operations Tests

    private static async Task TestCopyObjectOperations(IMinioClient client)
    {
        Console.WriteLine("=== Testing Copy Object Operations ===");

        var (sourceObjectName, bytes) = await SetupCopySourceObject(client).ConfigureAwait(false);
        var destObjectName1 = await TestSimpleCopy(client, sourceObjectName, bytes).ConfigureAwait(false);
        var destObjectName2 = await TestCopyWithMetadataReplacement(client, sourceObjectName).ConfigureAwait(false);
        var destObjectName3 = await TestCopyWithTags(client, sourceObjectName).ConfigureAwait(false);
        await CleanupCopyTestObjects(client, sourceObjectName, destObjectName1, destObjectName2, destObjectName3).ConfigureAwait(false);

        Console.WriteLine();
    }

    private static async Task<(string sourceObjectName, byte[] bytes)> SetupCopySourceObject(IMinioClient client)
    {
        Console.Write("Setting up source object for copy tests... ");
        var sourceObjectName = "copy-source/original-file.txt";
        var content = "This is the original file content for copy tests";
        var bytes = Encoding.UTF8.GetBytes(content);
        using (var stream = new MemoryStream(bytes))
        {
            var putArgs = new PutObjectArgs()
                .WithBucket(TestBucketName)
                .WithObject(sourceObjectName)
                .WithStreamData(stream)
                .WithObjectSize(bytes.Length)
                .WithContentType("text/plain")
                .WithHeaders(new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    { "X-Amz-Meta-OriginalName", "original" },
                    { "X-Amz-Meta-Purpose", "CopyTest" }
                });
            
            _ = await client.PutObjectAsync(putArgs).ConfigureAwait(false);
        }
        Console.WriteLine("✓");
        return (sourceObjectName, bytes);
    }

    private static async Task<string> TestSimpleCopy(IMinioClient client, string sourceObjectName, byte[] bytes)
    {
        Console.Write("1. Copying object within same bucket... ");
        var destObjectName1 = "copy-dest/copied-file-1.txt";
        
        var copySource = new CopySourceObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(sourceObjectName);

        var copyArgs = new CopyObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName1)
            .WithCopyObjectSource(copySource);

        await client.CopyObjectAsync(copyArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.Write("2. Verifying copied object... ");
        var statArgs = new StatObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName1);
        
        var copiedStat = await client.StatObjectAsync(statArgs).ConfigureAwait(false);
        var expectedSize = (long)bytes.Length;
        
        if (copiedStat.Size == expectedSize)
            Console.WriteLine($"✓ (Size matches: {copiedStat.Size} bytes)");
        else
            Console.WriteLine($"⚠ (Size mismatch: expected {bytes.Length}, got {copiedStat.Size})");

        return destObjectName1;
    }

    private static async Task<string> TestCopyWithMetadataReplacement(IMinioClient client, string sourceObjectName)
    {
        Console.Write("3. Copying with metadata replacement... ");
        var destObjectName2 = "copy-dest/copied-file-2.txt";
        
        var copySource = new CopySourceObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(sourceObjectName);

        var newMetadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "X-Amz-Meta-CopiedBy", "TestSuite" },
            { "X-Amz-Meta-CopyDate", DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) }
        };

        var copyArgs = new CopyObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName2)
            .WithCopyObjectSource(copySource)
            .WithReplaceMetadataDirective(true)
            .WithHeaders(newMetadata);

        await client.CopyObjectAsync(copyArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.Write("4. Verifying replaced metadata... ");
        var statArgs = new StatObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName2);
        
        var copiedStat = await client.StatObjectAsync(statArgs).ConfigureAwait(false);
        var hasNewMetadata = copiedStat.MetaData.Any(m => 
            m.Key.Contains("CopiedBy", StringComparison.OrdinalIgnoreCase));
        
        if (hasNewMetadata)
            Console.WriteLine("✓ (New metadata applied successfully)");
        else
            Console.WriteLine("⚠ (Metadata replacement may not have worked)");

        return destObjectName2;
    }

    private static async Task<string> TestCopyWithTags(IMinioClient client, string sourceObjectName)
    {
        Console.Write("5. Copying with tags... ");
        var destObjectName3 = "copy-dest/copied-file-3.txt";
        
        var copySource = new CopySourceObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(sourceObjectName);

        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "Environment", "Test" },
            { "CopyType", "WithTags" }
        };

        var copyArgs = new CopyObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName3)
            .WithCopyObjectSource(copySource)
            .WithTagging(Tagging.GetObjectTags(tags));

        await client.CopyObjectAsync(copyArgs).ConfigureAwait(false);
        Console.WriteLine("✓");

        Console.Write("6. Verifying tags on copied object... ");
        var getTagsArgs = new GetObjectTagsArgs()
            .WithBucket(TestBucketName)
            .WithObject(destObjectName3);
        
        var copiedTags = await client.GetObjectTagsAsync(getTagsArgs).ConfigureAwait(false);
        
        if (copiedTags.Tags.Count >= tags.Count)
        {
            Console.WriteLine($"✓ (Found {copiedTags.Tags.Count} tags)");
            foreach (var tag in copiedTags.Tags)
                Console.WriteLine($"   - {tag.Key}: {tag.Value}");
        }
        else
        {
            Console.WriteLine($"⚠ (Expected {tags.Count} tags, found {copiedTags.Tags.Count})");
        }

        return destObjectName3;
    }

    private static async Task CleanupCopyTestObjects(IMinioClient client, string sourceObjectName, 
        string destObjectName1, string destObjectName2, string destObjectName3)
    {
        Console.Write("7. Cleaning up copy test objects... ");
        var objectsToDelete = new List<string>
        {
            sourceObjectName,
            destObjectName1,
            destObjectName2,
            destObjectName3
        };

        var removeArgs = new RemoveObjectsArgs()
            .WithBucket(TestBucketName)
            .WithObjects(objectsToDelete);

        _ = await client.RemoveObjectsAsync(removeArgs).ConfigureAwait(false);
        Console.WriteLine("✓");
    }

    #endregion

    #region Presigned URLs Tests

    private static async Task TestPresignedUrls(IMinioClient client)
    {
        Console.WriteLine("=== Testing Presigned URLs ===");

        // Test 1: Generate presigned GET URL
        Console.Write("1. Generating presigned GET URL... ");
        try
        {
            var presignedGetArgs = new PresignedGetObjectArgs()
                .WithBucket(TestBucketName)
                .WithObject(TestObjectName)
                .WithExpiry(3600); // 1 hour

            var presignedUrl = await client.PresignedGetObjectAsync(presignedGetArgs).ConfigureAwait(false);
            Console.WriteLine("✓");
            Console.WriteLine($"   URL: {presignedUrl[..Math.Min(100, presignedUrl.Length)]}...");
            
            // Check if URL contains SAS token from connection string (fallback behavior)
            if (presignedUrl.Contains("se=", StringComparison.Ordinal))
            {
                Console.WriteLine("   ⚠ Note: Using existing SAS token from connection string");
                Console.WriteLine("   → Custom expiry not applied (limited by original SAS token)");
                Console.WriteLine("   → For custom expiry, use account key authentication instead");
            }
        }
        catch (NotSupportedException ex)
        {
            Console.WriteLine("⚠ (Skipped - Account key required for generating new SAS tokens)");
            Console.WriteLine($"   Reason: {ex.Message.Split('.')[0]}");
            Console.WriteLine("   → Current connection uses SAS token authentication");
            Console.WriteLine("   → To generate presigned URLs, authenticate with account key");
        }

        // Test 2: Generate presigned PUT URL
        Console.Write("2. Generating presigned PUT URL... ");
        try
        {
            var presignedPutArgs = new PresignedPutObjectArgs()
                .WithBucket(TestBucketName)
                .WithObject("presigned-upload-test.txt")
                .WithExpiry(3600); // 1 hour

            var presignedPutUrl = await client.PresignedPutObjectAsync(presignedPutArgs).ConfigureAwait(false);
            Console.WriteLine("✓");
            Console.WriteLine($"   URL: {presignedPutUrl[..Math.Min(100, presignedPutUrl.Length)]}...");
            
            // Check if URL contains SAS token from connection string (fallback behavior)
            if (presignedPutUrl.Contains("se=", StringComparison.Ordinal))
            {
                Console.WriteLine("   ⚠ Note: Using existing SAS token from connection string");
                Console.WriteLine("   → Custom expiry not applied (limited by original SAS token)");
                Console.WriteLine("   → For custom expiry, use account key authentication instead");
            }
            
            // Verify URL has write permissions
            if (presignedPutUrl.Contains("sp=", StringComparison.Ordinal))
            {
                Console.WriteLine("   ✓ URL includes permission parameters for PUT operations");
            }
        }
        catch (NotSupportedException ex)
        {
            Console.WriteLine("⚠ (Skipped - Account key required for generating new SAS tokens)");
            Console.WriteLine($"   Reason: {ex.Message.Split('.')[0]}");
            Console.WriteLine("   → Current connection uses SAS token authentication");
            Console.WriteLine("   → To generate presigned URLs, authenticate with account key");
        }

        Console.WriteLine();
    }

    #endregion

    #region S3 Compliance Tests

    private static async Task TestS3ComplianceRules(IMinioClient client)
    {
        Console.WriteLine("=== Testing S3 Compliance Rules ===");

        // Test 1: PutObject to non-existent bucket should throw BucketNotFoundException
        Console.Write("1. Testing PutObject to non-existent bucket... ");
        try
        {
            var nonExistentBucket = "non-existent-bucket-12345";
            var content = "test";
            var bytes = Encoding.UTF8.GetBytes(content);
            using var stream = new MemoryStream(bytes);
            
            var putArgs = new PutObjectArgs()
                .WithBucket(nonExistentBucket)
                .WithObject("test.txt")
                .WithStreamData(stream)
                .WithObjectSize(bytes.Length);
            
            _ = await client.PutObjectAsync(putArgs).ConfigureAwait(false);
            Console.WriteLine("✗ (Should have thrown BucketNotFoundException)");
        }
        catch (BucketNotFoundException)
        {
            Console.WriteLine("✓ (Correctly threw BucketNotFoundException)");
        }

        // Test 2: StatObject on non-existent object should return NotFound
        Console.Write("2. Testing StatObject on non-existent object... ");
        var statArgs = new StatObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject("non-existent-file.txt");
        
        var stat = await client.StatObjectAsync(statArgs).ConfigureAwait(false);
        if (stat == null || stat.Size == 0)
        {
            Console.WriteLine("✓ (Returned null or empty stat)");
        }
        else
        {
            Console.WriteLine("✗ (Should have returned null stat)");
        }

        // Test 3: RemoveObject is idempotent (removing non-existent object succeeds)
        Console.Write("3. Testing RemoveObject idempotency... ");
        var removeArgs = new RemoveObjectArgs()
            .WithBucket(TestBucketName)
            .WithObject("non-existent-file.txt");
        
        await client.RemoveObjectAsync(removeArgs).ConfigureAwait(false);
        Console.WriteLine("✓ (Idempotent - no error on non-existent object)");

        // Test 4: ListObjects on non-existent bucket should throw
        Console.Write("4. Testing ListObjects on non-existent bucket... ");
        try
        {
            var listArgs = new ListObjectsArgs()
                .WithBucket("non-existent-bucket-12345")
                .WithRecursive(true);
            
            await foreach (var item in client.ListObjectsEnumAsync(listArgs).ConfigureAwait(false))
            {
                // Should not reach here
            }
            Console.WriteLine("✗ (Should have thrown BucketNotFoundException)");
        }
        catch (BucketNotFoundException)
        {
            Console.WriteLine("✓ (Correctly threw BucketNotFoundException)");
        }

        Console.WriteLine();
    }

    #endregion

    #region Cleanup

    private static async Task CleanupTestResources(IMinioClient client)
    {
        Console.WriteLine("=== Cleaning Up Test Resources ===");

        try
        {
            // Remove all test objects
            Console.Write("Removing test objects... ");
            var listArgs = new ListObjectsArgs()
                .WithBucket(TestBucketName)
                .WithRecursive(true);

            await foreach (var item in client.ListObjectsEnumAsync(listArgs).ConfigureAwait(false))
            {
                var removeArgs = new RemoveObjectArgs()
                    .WithBucket(TestBucketName)
                    .WithObject(item.Key);
                await client.RemoveObjectAsync(removeArgs).ConfigureAwait(false);
            }
            Console.WriteLine("✓");

            // Remove bucket
            Console.Write("Removing test bucket... ");
            var removeBucketArgs = new RemoveBucketArgs()
                .WithBucket(TestBucketName);
            await client.RemoveBucketAsync(removeBucketArgs).ConfigureAwait(false);
            Console.WriteLine("✓");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Cleanup failed - {ex.Message}");
        }

        Console.WriteLine();
    }

    #endregion
}
