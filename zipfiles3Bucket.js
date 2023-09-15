const AWS = require('aws-sdk');
const S3 = new AWS.S3({
  apiVersion: '2006-03-01',
  signatureVersion: 'v4',
  httpOptions: {
    timeout: 300000 // 5min Should Match Lambda function timeout
  }
});
const archiver = require('archiver');
import stream from 'stream';

const UPLOAD_BUCKET_NAME = "my-s3-bucket";
const URL_EXPIRE_TIME = 5*60;

export async function getZipSignedUrl(event) {
  const prefix = `uploads/id123123/}`;   //replace this with your S3 prefix
  let files = ["12314123.png", "56787567.png"]  //replace this with your files

  if (files.length == 0) {
    console.log("No files to zip");
    return result(404, "No pictures to download");
  }
  console.log("Files to zip: ", files);

  try {
    files = files.map(file => {
        return {
            fileName: file,
            key: prefix + '/' + file,
            type: "file"
        };
    });
    const destinationKey = prefix + '/' + 'uploads.zip'
    console.log("files: ", files);
    console.log("destinationKey: ", destinationKey);

    await streamToZipInS3(files, destinationKey);
    const presignedUrl = await getSignedUrl(UPLOAD_BUCKET_NAME, destinationKey, URL_EXPIRE_TIME, "uploads.zip");
    console.log("presignedUrl: ", presignedUrl);

    if (!presignedUrl) {
      return result(500, null);
    }
    return result(200, presignedUrl);
  }
  catch(error) {
    console.error(`Error: ${error}`);
    return result(500, null);
  }
}

// Helper functions

export function result(code, message) {
  return {
    statusCode: code,
    body: JSON.stringify(
      {
        message: message
      }
    )
  }
}

export async function streamToZipInS3(files, destinationKey) {
  await new Promise(async (resolve, reject) => {
    var zipStream = streamTo(UPLOAD_BUCKET_NAME, destinationKey, resolve);
    zipStream.on("error", reject);

    var archive = archiver("zip");
    archive.on("error", err => {
      throw new Error(err);
    });
    archive.pipe(zipStream);

    for (const file of files) {
      if (file["type"] == "file") {
        archive.append(getStream(UPLOAD_BUCKET_NAME, file["key"]), {
          name: file["fileName"]
        });
      }
    }
    archive.finalize();
  })
  .catch(err => {
    console.log(err);
    throw new Error(err);
  });
}

function streamTo(bucket, key, resolve) {
  var passthrough = new stream.PassThrough();
  S3.upload(
    {
      Bucket: bucket,
      Key: key,
      Body: passthrough,
      ContentType: "application/zip",
      ServerSideEncryption: "AES256"
    },
    (err, data) => {
      if (err) {
        console.error('Error while uploading zip')
        throw new Error(err);
        reject(err)
        return
      }
      console.log('Zip uploaded')
      resolve()
    }
  ).on("httpUploadProgress", progress => {
    console.log(progress)
  });
  return passthrough;
}

function getStream(bucket, key) {
  let streamCreated = false;
  const passThroughStream = new stream.PassThrough();

  passThroughStream.on("newListener", event => {
    if (!streamCreated && event == "data") {
      const s3Stream = S3
        .getObject({ Bucket: bucket, Key: key })
        .createReadStream();
      s3Stream
        .on("error", err => passThroughStream.emit("error", err))
        .pipe(passThroughStream);

      streamCreated = true;
    }
  });

  return passThroughStream;
}

export async function getSignedUrl(bucket: string, key: string, expires: number, downloadFilename?: string): Promise<string> {
    const exists = await objectExists(bucket, key);
    if (!exists) {
        console.info(`Object ${bucket}/${key} does not exists`);
        return null
    }

    let params = {
        Bucket: bucket,
        Key: key,
        Expires: expires,
    };
    if (downloadFilename) {
        params['ResponseContentDisposition'] = `inline; filename="${encodeURIComponent(downloadFilename)}"`; 
    }
    
    try {
        const url = s3.getSignedUrl('getObject', params);
        return url;
    } catch (err) {
        console.error(`Unable to get URL for ${bucket}/${key}`, err);
        return null;
    }
};
