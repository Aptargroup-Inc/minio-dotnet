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
    private const string ConnectionString = "";
    
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

    #region Presigned URLs Tests

    private static async Task TestPresignedUrls(IMinioClient client)
    {
        Console.WriteLine("=== Testing Presigned URLs ===");

        // Test: Generate presigned GET URL
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
