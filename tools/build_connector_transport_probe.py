# Copyright 2026 Unity Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Extract the sibling connector frame reader into a standalone C# stress probe.

Compile the generated test-results/ConnectorProbe.cs with a C# compiler and run it.
The probe validates payload integrity and reproduces the existing EOF polling bug.
No Unity installation is needed; this does not test Unity application dispatch.
"""

from pathlib import Path
repo = Path(__file__).resolve().parents[1]
connector = repo.parent / 'ros-tcp-connector'
source = (connector / 'com.unity.robotics.ros-tcp-connector/Runtime/TcpConnector/ROSConnection.cs').read_text()
start=source.index('        static async Task ReadToByteArray(')
end=source.index('        void OnApplicationQuit()',start)
methods=source[start:end]
harness=r'''
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
class ConnectorProbe {
METHODS
static byte[] Frame(string topic, byte[] payload) {
 using(var ms=new MemoryStream()) { using(var bw=new BinaryWriter(ms)) {
 var name=Encoding.ASCII.GetBytes(topic); bw.Write(name.Length); bw.Write(name);
 bw.Write(payload.Length); bw.Write(payload); return ms.ToArray(); } }
}
static void Check(bool condition,string message) { if(!condition) throw new Exception(message); }
static void Main() { Run().GetAwaiter().GetResult(); }
static async Task Run() {
 var listener=new TcpListener(IPAddress.Loopback,0); listener.Start();
 var receiver=new TcpClient(); var connect=receiver.ConnectAsync(IPAddress.Loopback,((IPEndPoint)listener.LocalEndpoint).Port);
 var sender=await listener.AcceptTcpClientAsync(); await connect; listener.Stop();
 sender.NoDelay=true; receiver.NoDelay=true;
 using(receiver) using(sender) using(var cancel=new CancellationTokenSource()) {
 var input=receiver.GetStream(); var output=sender.GetStream(); input.ReadTimeout=150;
 // Start a read before a pause longer than the proposed executor wait.
 var idleRead=ReadMessageContents(input,1,cancel.Token); await Task.Delay(1500);
 Check(!idleRead.IsCompleted,"Idle read unexpectedly completed");
 byte[] empty=Frame("",new byte[0]); await output.WriteAsync(empty,0,empty.Length);
 var keepalive=await idleRead; Check(keepalive.Item1=="" && keepalive.Item2.Length==0,"Keepalive mismatch");
 // Every byte can arrive separately, including lengths and action framing.
 string[] topics={"__action_goal_response","__action_result","/fibonacci",new string('x',300)};
 for(int i=0;i<topics.Length;i++) {
  byte[] payload=Encoding.ASCII.GetBytes("fragmented-payload-"+i); var read=ReadMessageContents(input,1,cancel.Token);
  byte[] frame=Frame(topics[i],payload);
  for(int j=0;j<frame.Length;j++) { await output.WriteAsync(frame,j,1); if(j%7==0) await Task.Delay(1); }
  var got=await read; Check(got.Item1==topics[i] && got.Item2.SequenceEqual(payload),"Fragmented frame corrupted");
 }
 // Large frames and a sustained burst, read concurrently to exercise buffering.
 var burstReader=Task.Run(async delegate {
  for(int i=0;i<1000;i++) { var got=await ReadMessageContents(input,1,cancel.Token);
   Check(got.Item1=="/burst/"+i,"Burst order mismatch");
   int size=i%100==0?1048576:4096; Check(got.Item2.Length==size,"Burst size mismatch");
   for(int j=0;j<size;j++) Check(got.Item2[j]==(byte)(i+j),"Burst payload mismatch");
  }
 });
 for(int i=0;i<1000;i++) { int size=i%100==0?1048576:4096; var payload=new byte[size];
  for(int j=0;j<size;j++) payload[j]=(byte)(i+j); var frame=Frame("/burst/"+i,payload); await output.WriteAsync(frame,0,frame.Length);
 }
 await burstReader; Console.WriteLine("PASS: idle 1.5s, keepalive, byte-fragmented action frames, 1000 ordered frames including ten 1 MiB payloads");
 var eofRead=ReadMessageContents(input,1,cancel.Token); sender.Client.Shutdown(SocketShutdown.Send);
 await Task.Delay(600); Console.WriteLine("EOF observed within 600ms: "+eofRead.IsCompleted+" (ReadTimeout=150ms)");
 Check(!eofRead.IsCompleted,"EOF behavior changed; inspect result");
 cancel.Cancel(); try { await eofRead; throw new Exception("Cancellation missing"); } catch(OperationCanceledException) {}
 Console.WriteLine("PASS: cancellation releases pending read. CONFIRMED EXISTING ISSUE: peer half-close leaves reader polling past ReadTimeout.");
 }
}
}
'''.replace('METHODS',methods)
output = repo / 'test-results/ConnectorProbe.cs'
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(harness)
print(output)
