import express from 'express';
import { google } from 'googleapis';
import path from 'path';
import { fileURLToPath } from 'url';
import { Readable, Transform } from 'stream';
import fs from 'fs';
import os from 'os';
import crypto from 'crypto';
import unzipper from 'unzipper';
import mime from 'mime-types';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());
app.use(express.static('public'));

// Serve Firebase config
app.get('/firebase-applet-config.json', (req, res) => {
  res.sendFile(path.join(__dirname, 'firebase-applet-config.json'));
});

// Helper to get Drive client
const getDriveClient = (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    throw new Error('Unauthorized');
  }
  const token = authHeader.split(' ')[1];
  const oauth2Client = new google.auth.OAuth2();
  oauth2Client.setCredentials({ access_token: token });
  return google.drive({ version: 'v3', auth: oauth2Client });
};

// 1. Get Folders
app.get('/api/folders', async (req, res) => {
  try {
    const drive = getDriveClient(req, res);
    const response = await drive.files.list({
      q: "mimeType='application/vnd.google-apps.folder' and trashed=false",
      fields: 'files(id, name, parents)',
      pageSize: 1000,
    });
    
    const folders = response.data.files || [];
    const folderMap = new Map();
    folders.forEach(f => folderMap.set(f.id, f));

    folders.forEach(f => {
      let path = f.name;
      let current = f;
      const seen = new Set([f.id]);
      while (current.parents && current.parents.length > 0) {
        const parentId = current.parents[0];
        if (seen.has(parentId)) break; // avoid loops
        seen.add(parentId);
        const parent = folderMap.get(parentId);
        if (parent) {
          path = parent.name + ' / ' + path;
          current = parent;
        } else {
          break;
        }
      }
      f.path = path;
    });

    // Sort folders by path alphabetically
    folders.sort((a, b) => a.path.localeCompare(b.path));

    res.json({ success: true, folders: folders });
  } catch (error) {
    console.error('Fetch folders error:', error);
    res.status(500).json({ error: error.message });
  }
});

// 2. Create Folder
app.post('/api/create-folder', async (req, res) => {
  try {
    const drive = getDriveClient(req, res);
    const { folderName } = req.body;
    if (!folderName) throw new Error('Folder name is required');
    
    const fileMetadata = {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder',
    };
    
    const response = await drive.files.create({
      requestBody: fileMetadata,
      fields: 'id, name',
    });
    res.json({ success: true, folder: response.data });
  } catch (error) {
    console.error('Create folder error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Helper for retrying fetches
async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      if ([502, 503, 504].includes(response.status)) {
        if (i === retries - 1) throw new Error(`Failed to fetch file: ${response.status}`);
        await new Promise(r => setTimeout(r, 2000 * (i + 1))); // backoff
        continue;
      }
      return response; // let the caller handle other non-ok statuses
    } catch (error) {
      if (i === retries - 1) throw error;
      await new Promise(r => setTimeout(r, 2000 * (i + 1)));
    }
  }
}

// 3. Check File (Size & Format)
app.post('/api/check-file', async (req, res) => {
  try {
    const { fileUrl } = req.body;
    if (!fileUrl) throw new Error('fileUrl is required');
    
    let mimeType = 'Unknown';
    let size = null;
    let headSuccess = false;

    // Perform a HEAD request to get metadata without downloading
    try {
      const response = await fetchWithRetry(fileUrl, { method: 'HEAD' });
      if (response.ok) {
        mimeType = response.headers.get('content-type') || 'Unknown';
        size = response.headers.get('content-length');
        headSuccess = true;
      }
    } catch (e) {
      // HEAD request failed, silently fall back to GET
    }

    if (!headSuccess) {
        // Fallback to GET if HEAD is not allowed or failed
        const getResponse = await fetchWithRetry(fileUrl);
        if (!getResponse.ok) throw new Error(`Failed to fetch file: ${getResponse.status}`);
        
        mimeType = getResponse.headers.get('content-type') || 'Unknown';
        size = getResponse.headers.get('content-length');
        getResponse.body.cancel(); // Stop download immediately
    }
    
    res.json({
      success: true,
      mimeType: mimeType,
      size: size,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 4. Upload with Progress Stream
app.post('/api/upload-stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Transfer-Encoding', 'chunked');
  
  const sendUpdate = (data) => {
    res.write(JSON.stringify(data) + '\n');
  };

  try {
    const drive = getDriveClient(req, res);
    const { fileUrl, fileName, folderId } = req.body;
    if (!fileUrl) throw new Error('fileUrl is required');

    sendUpdate({ type: 'status', message: 'فائل ڈاؤنلوڈ ہو رہی ہے...' }); // Fetching file
    
    const response = await fetchWithRetry(fileUrl);
    if (!response.ok) throw new Error(`Failed to fetch file: ${response.status}`);
    
    const mimeType = response.headers.get('content-type') || 'application/octet-stream';
    const totalSizeStr = response.headers.get('content-length');
    const totalSize = totalSizeStr ? parseInt(totalSizeStr, 10) : 0;
    
    let actualFileName = fileName;
    if (!actualFileName) {
        const contentDisposition = response.headers.get('content-disposition');
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
            if (filenameMatch) actualFileName = filenameMatch[1];
        }
    }
    if (!actualFileName) {
        actualFileName = fileUrl.split('/').pop().split('?')[0] || 'Downloaded_File';
    }

    let downloaded = 0;
    let lastReportTime = 0;
    
    const progressStream = new Transform({
      transform(chunk, encoding, callback) {
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastReportTime > 300) { // report every 300ms
          if (totalSize > 0) {
            sendUpdate({ type: 'progress', percent: Math.round((downloaded / totalSize) * 100), bytes: downloaded, total: totalSize });
          } else {
            sendUpdate({ type: 'progress', bytes: downloaded });
          }
          lastReportTime = now;
        }
        callback(null, chunk);
      }
    });

    const nodeStream = Readable.fromWeb(response.body).pipe(progressStream);

    const requestBody = { name: actualFileName };
    if (folderId && folderId !== 'root') {
      requestBody.parents = [folderId];
    }

    sendUpdate({ type: 'status', message: 'ڈرائیو میں اپلوڈ ہو رہی ہے...' }); // Uploading to Drive

    const driveResponse = await drive.files.create({
      requestBody,
      media: { mimeType, body: nodeStream },
      fields: 'id, name, webViewLink'
    });

    sendUpdate({ type: 'success', file: driveResponse.data });
    res.end();
  } catch (error) {
    console.error('Upload Error:', error);
    sendUpdate({ type: 'error', message: error.message });
    res.end();
  }
});

// Helper for recursive folder upload
const uploadDirectoryToDrive = async (drive, localDirPath, parentDriveFolderId, sendUpdate) => {
  const items = await fs.promises.readdir(localDirPath, { withFileTypes: true });
  for (const item of items) {
    const localItemPath = path.join(localDirPath, item.name);
    
    // Ignore hidden files like .DS_Store
    if (item.name.startsWith('.')) continue;

    if (item.isDirectory()) {
      sendUpdate({ type: 'status', message: `فولڈر بن رہا ہے: ${item.name}` });
      // Create folder in Drive
      const folderMetadata = {
        name: item.name,
        mimeType: 'application/vnd.google-apps.folder',
      };
      if (parentDriveFolderId && parentDriveFolderId !== 'root') {
        folderMetadata.parents = [parentDriveFolderId];
      }
      const folderResponse = await drive.files.create({
        requestBody: folderMetadata,
        fields: 'id',
      });
      const newFolderId = folderResponse.data.id;
      // Recursively upload directory
      await uploadDirectoryToDrive(drive, localItemPath, newFolderId, sendUpdate);
    } else {
      sendUpdate({ type: 'status', message: `فائل اپلوڈ ہو رہی ہے: ${item.name}` });
      // Upload file
      const mimeType = mime.lookup(localItemPath) || 'application/octet-stream';
      const fileMetadata = { name: item.name };
      if (parentDriveFolderId && parentDriveFolderId !== 'root') {
        fileMetadata.parents = [parentDriveFolderId];
      }
      
      const fileStream = fs.createReadStream(localItemPath);
      await drive.files.create({
        requestBody: fileMetadata,
        media: { mimeType, body: fileStream },
        fields: 'id, name',
      });
    }
  }
};

// 5. Upload and Extract ZIP Stream
app.post('/api/upload-zip-stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Transfer-Encoding', 'chunked');
  
  const sendUpdate = (data) => {
    res.write(JSON.stringify(data) + '\n');
  };

  const extractDir = path.join(os.tmpdir(), crypto.randomUUID());

  try {
    const drive = getDriveClient(req, res);
    const { fileUrl, folderId } = req.body;
    if (!fileUrl) throw new Error('fileUrl is required');

    sendUpdate({ type: 'status', message: 'زپ فائل ڈاؤنلوڈ اور ایکسٹراکٹ ہو رہی ہے...' });
    
    const response = await fetchWithRetry(fileUrl);
    if (!response.ok) throw new Error(`Failed to fetch file: ${response.status}`);
    
    const totalSizeStr = response.headers.get('content-length');
    const totalSize = totalSizeStr ? parseInt(totalSizeStr, 10) : 0;
    
    let downloaded = 0;
    let lastReportTime = 0;
    
    const progressStream = new Transform({
      transform(chunk, encoding, callback) {
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastReportTime > 300) { // report every 300ms
          if (totalSize > 0) {
            sendUpdate({ type: 'progress', percent: Math.round((downloaded / totalSize) * 100), bytes: downloaded, total: totalSize });
          } else {
            sendUpdate({ type: 'progress', bytes: downloaded });
          }
          lastReportTime = now;
        }
        callback(null, chunk);
      }
    });

    // Convert Web Stream to Node Stream
    const nodeStream = Readable.fromWeb(response.body).pipe(progressStream);

    // Create temp directory for extraction
    await fs.promises.mkdir(extractDir, { recursive: true });

    // Extract zip
    await new Promise((resolve, reject) => {
      nodeStream.pipe(unzipper.Extract({ path: extractDir }))
        .on('close', resolve)
        .on('error', reject);
    });

    sendUpdate({ type: 'status', message: 'ایکسٹریکٹ مکمل! ڈرائیو میں اپلوڈ شروع ہو رہا ہے...' });
    
    // Upload extracted files
    await uploadDirectoryToDrive(drive, extractDir, folderId, sendUpdate);

    sendUpdate({ type: 'success', message: 'زپ فائل کے تمام مندرجات کامیابی سے اپلوڈ ہو گئے۔', isZip: true });
    res.end();
  } catch (error) {
    console.error('ZIP Upload Error:', error);
    sendUpdate({ type: 'error', message: error.message });
    res.end();
  } finally {
    // Clean up temp directory
    try {
      await fs.promises.rm(extractDir, { recursive: true, force: true });
    } catch (e) {
      console.error('Failed to clean up temp dir:', e);
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server is running on port ${PORT}`);
});
