using System.Collections.Concurrent;
using System.Diagnostics;

namespace V380Decoder.src
{
    public class H264Transcoder : IDisposable
    {
        private readonly object inputLock = new();
        private readonly ConcurrentQueue<ulong> timestamps = new();
        private readonly Action<FrameData> onFrame;
        private readonly List<byte> stdoutBuffer = new();
        private readonly List<byte[]> currentAccessUnit = new();
        private bool currentAccessUnitHasVcl;
        private Process process;
        private Thread stdoutThread;
        private Thread stderrThread;
        private bool disposed;
        private ulong generatedTimestamp;

        private readonly object paramLock = new();
        private byte[] vpsNal;
        private byte[] spsNal;
        private byte[] ppsNal;

        public bool IsAvailable { get; private set; }

        public H264Transcoder(Action<FrameData> onFrame)
        {
            this.onFrame = onFrame;
            Start();
        }

        public void PushFrame(FrameData frame)
        {
            if (!IsAvailable || disposed || frame?.Payload == null || frame.Payload.Length == 0)
                return;

            byte[] payload = CacheAndInjectParams(frame.Payload);

            timestamps.Enqueue(frame.Timestamp);
            lock (inputLock)
            {
                try
                {
                    process.StandardInput.BaseStream.Write(payload, 0, payload.Length);
                    process.StandardInput.BaseStream.Flush();
                }
                catch (Exception ex)
                {
                    IsAvailable = false;
                    LogUtils.debug($"[RTSP-XCODE] stdin write failed: {ex.Message}");
                }
            }
        }

        // Some HEVC cameras only emit VPS/SPS/PPS once (or intermittently), not on
        // every IDR. ffmpeg's raw hevc decoder parses whatever byte stream we hand
        // it and can't decode an IDR without those parameter sets - it silently logs
        // "missing picture in access unit" and never produces output, so the RTSP
        // stream hangs forever. Cache the params the moment we see them and prepend
        // them to every IDR access unit, same trick SnapshotManager already uses to
        // make single-shot JPEG snapshots work for the same cameras.
        private byte[] CacheAndInjectParams(byte[] payload)
        {
            bool isIdr = false;
            RtspServer.ParseNals(payload, VideoCodec.H265, (nalType, nal) =>
            {
                switch (nalType)
                {
                    case 32: lock (paramLock) vpsNal = nal; break;
                    case 33: lock (paramLock) spsNal = nal; break;
                    case 34: lock (paramLock) ppsNal = nal; break;
                    case 19:
                    case 20: isIdr = true; break;
                }
            });

            if (!isIdr)
                return payload;

            byte[] vps, sps, pps;
            lock (paramLock) { vps = vpsNal; sps = spsNal; pps = ppsNal; }
            if (sps == null || pps == null)
                return payload;

            byte[] startCode = { 0, 0, 0, 1 };
            using var ms = new MemoryStream();
            if (vps != null) { ms.Write(startCode, 0, 4); ms.Write(vps, 0, vps.Length); }
            ms.Write(startCode, 0, 4); ms.Write(sps, 0, sps.Length);
            ms.Write(startCode, 0, 4); ms.Write(pps, 0, pps.Length);
            ms.Write(payload, 0, payload.Length);
            return ms.ToArray();
        }

        private void Start()
        {
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = "ffmpeg",
                    Arguments = "-hide_banner -loglevel error -fflags nobuffer -f hevc -i pipe:0 -an -c:v libx264 -preset ultrafast -tune zerolatency -bf 0 -g 30 -keyint_min 30 -sc_threshold 0 -pix_fmt yuv420p -x264-params aud=1:repeat-headers=1 -f h264 pipe:1",
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                process = Process.Start(psi);
                if (process == null)
                    return;

                IsAvailable = true;
                stdoutThread = new Thread(ReadStdoutLoop) { IsBackground = true, Name = "rtsp-xcode-stdout" };
                stderrThread = new Thread(ReadStderrLoop) { IsBackground = true, Name = "rtsp-xcode-stderr" };
                stdoutThread.Start();
                stderrThread.Start();
                LogUtils.debug("[RTSP-XCODE] ffmpeg transcoder started");
            }
            catch (Exception ex)
            {
                IsAvailable = false;
                LogUtils.debug($"[RTSP-XCODE] failed to start ffmpeg: {ex.Message}");
            }
        }

        private void ReadStdoutLoop()
        {
            var chunk = new byte[8192];
            try
            {
                while (!disposed && process != null && !process.HasExited)
                {
                    int n = process.StandardOutput.BaseStream.Read(chunk, 0, chunk.Length);
                    if (n <= 0) break;

                    lock (stdoutBuffer)
                    {
                        for (int i = 0; i < n; i++) stdoutBuffer.Add(chunk[i]);
                        DrainStdoutBuffer();
                    }
                }
            }
            catch (Exception ex)
            {
                if (!disposed)
                    LogUtils.debug($"[RTSP-XCODE] stdout failed: {ex.Message}");
            }
            finally
            {
                lock (stdoutBuffer)
                {
                    FlushCurrentAccessUnit();
                }
            }
        }

        private void ReadStderrLoop()
        {
            try
            {
                while (!disposed && process != null && !process.HasExited)
                {
                    var line = process.StandardError.ReadLine();
                    if (line == null) break;
                    LogUtils.debug($"[RTSP-XCODE] {line}");
                }
            }
            catch { }
        }

        private void DrainStdoutBuffer()
        {
            int processed = 0;
            while (true)
            {
                int start = FindStartCode(stdoutBuffer, processed);
                if (start < 0) break;

                int next = FindStartCode(stdoutBuffer, start + 3);
                if (next < 0) break;

                var nal = stdoutBuffer.GetRange(start, next - start).ToArray();
                ProcessNal(nal);
                processed = next;
            }

            if (processed > 0)
                stdoutBuffer.RemoveRange(0, processed);
        }

        private void ProcessNal(byte[] nal)
        {
            int scLen = nal.Length >= 4 && nal[2] == 1 ? 3 : 4;
            if (nal.Length <= scLen) return;

            int nalType = nal[scLen] & 0x1F;
            if (nalType == 9)
            {
                FlushCurrentAccessUnit();
                return;
            }

            bool isVcl = nalType is 1 or 5;
            if (isVcl && currentAccessUnitHasVcl)
            {
                FlushCurrentAccessUnit();
            }

            currentAccessUnit.Add(nal);
            if (isVcl)
                currentAccessUnitHasVcl = true;
        }

        private void FlushCurrentAccessUnit()
        {
            if (currentAccessUnit.Count == 0) return;

            int len = currentAccessUnit.Sum(x => x.Length);
            byte[] payload = new byte[len];
            int offset = 0;
            foreach (var nal in currentAccessUnit)
            {
                Array.Copy(nal, 0, payload, offset, nal.Length);
                offset += nal.Length;
            }

            currentAccessUnit.Clear();
            currentAccessUnitHasVcl = false;
            timestamps.TryDequeue(out var ts);
            if (ts == 0)
                ts = ++generatedTimestamp;

            onFrame(new FrameData
            {
                RawType = 0x00,
                Timestamp = ts,
                Codec = VideoCodec.H264,
                Payload = payload
            });
        }

        private static int FindStartCode(List<byte> data, int from)
        {
            for (int i = from; i + 3 < data.Count; i++)
            {
                if (data[i] == 0 && data[i + 1] == 0)
                {
                    if (data[i + 2] == 1) return i;
                    if (data[i + 2] == 0 && data[i + 3] == 1) return i;
                }
            }

            return -1;
        }

        public void Dispose()
        {
            disposed = true;
            IsAvailable = false;
            try { process?.StandardInput.Close(); } catch { }
            try
            {
                if (process is { HasExited: false })
                    process.Kill(true);
            }
            catch { }
            try { process?.Dispose(); } catch { }
        }
    }
}
