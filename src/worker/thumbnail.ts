import { File } from '@prisma/client';
import { spawn } from 'child_process';
import ffmpeg from 'ffmpeg-static';
import ffprobe from 'ffprobe-static';
import { createWriteStream } from 'fs';
import { rm } from 'fs/promises';
import config from 'lib/config';
import datasource from 'lib/datasource';
import Logger from 'lib/logger';
import prisma from 'lib/prisma';
import { join } from 'path';
import { isMainThread, workerData } from 'worker_threads';

const { id } = workerData as { id: number };

const logger = Logger.get('worker::thumbnail').child(id.toString() ?? 'unknown-ident');

if (isMainThread) {
  logger.error('worker is not a thread');
  process.exit(1);
}

async function loadThumbnail(path) {
  const args = ['-i', path, '-frames:v', '1', '-f', 'mjpeg', 'pipe:1'];

  const child = spawn(ffmpeg, args, { stdio: ['ignore', 'pipe', 'ignore'] });

  const data: Promise<Buffer> = new Promise((resolve, reject) => {
    child.stdout.once('data', resolve);
    child.once('error', reject);
  });

  return data;
}

async function loadGif(vidPath) {
  const probeArgs = [
    '-i',
    vidPath,
    '-v',
    'error',
    '-select_streams',
    'v:0',
    '-count_frames',
    '-show_entries',
    'stream=nb_read_frames,r_frame_rate',
    '-of',
    'csv=p=0',
  ];

  const child = spawn(ffprobe.path, probeArgs, { stdio: ['ignore', 'pipe', 'ignore'] });

  let data = '';
  child.stdout.on('data', (chunk) => {
    data += chunk;
  });
  await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', resolve);
  });

  const [fps_, frames_]: string[] = data.split('/');
  const fps: number = parseInt(fps_.replace(/\,/g, ''));
  const frames: number = parseInt(frames_.replace(/\,/g, ''));

  if (fps * 5 > frames) return null;

  const length = Math.floor(frames / (fps * 5));

  const gifArgs = ['-i', vidPath, '-frames:v', length, '-f', 'gif', 'pipe:1'];
  const child2 = spawn(ffmpeg, gifArgs, { stdio: ['ignore', 'pipe', 'ignore'] });

  let buffer = Buffer.alloc(0);
  child2.stdout.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
  });

  await new Promise((resolve, reject) => {
    child2.once('error', reject);
    child2.once('exit', resolve);
  });

  return buffer;
}

async function loadFileTmp(file: File, type = 'thumb') {
  const stream = await datasource.get(file.name);

  // pipe to tmp file
  const tmpFile = join(config.core.temp_directory, `zipline_${type}_${file.id}_${file.id}.tmp`);
  const fileWriteStream = createWriteStream(tmpFile);

  await new Promise((resolve, reject) => {
    stream.pipe(fileWriteStream);
    stream.once('error', reject);
    stream.once('end', resolve);
  });

  fileWriteStream.end();

  return tmpFile;
}

async function start() {
  const file = await prisma.file.findUnique({
    where: {
      id,
    },
    include: {
      thumbnail: true,
    },
  });

  if (!file) {
    logger.error('file not found');
    process.exit(1);
  }

  if (!file.mimetype.startsWith('video/')) {
    logger.info('file is not a video');
    process.exit(0);
  }

  if (file.thumbnail) {
    logger.info('thumbnail already exists');
    process.exit(0);
  }

  const tmpFileThumb = await loadFileTmp(file);
  const tmpFileGif = await loadFileTmp(file, 'gif');
  logger.debug(`loaded file to tmp: ${tmpFileThumb}`);
  const thumbnail = await loadThumbnail(tmpFileThumb);
  logger.debug(`loaded thumbnail: ${thumbnail.length} bytes mjpeg`);
  const gif = await loadGif(tmpFileGif);
  if (!gif) logger.debug(`no gif generated for ${file.id}`);
  else logger.debug(`loaded gif: ${gif.length} bytes gif`);

  const { thumbnail: thumb } = await prisma.file.update({
    where: {
      id: file.id,
    },
    data: {
      thumbnail: {
        create: {
          ...{
            name: `.thumb-${file.id}.jpg`,
          },
          ...(!!gif && {
            gif: `.clip-${file.id}.gif`,
          }),
        },
      },
    },
    select: {
      thumbnail: true,
    },
  });

  await datasource.save(thumb.name, thumbnail);
  if (gif) {
    await datasource.save(thumb.gif, gif);
    logger.info(`gif saved - ${thumb.gif}`);
  }

  logger.info(`thumbnail saved - ${thumb.name}`);
  logger.debug(`thumbnail ${JSON.stringify(thumb)}`);

  logger.debug(`removing tmp file: ${tmpFileThumb} ${gif ? tmpFileGif : ''}`);
  await rm(tmpFileThumb);
  if (gif) await rm(tmpFileGif);

  process.exit(0);
}

start();
