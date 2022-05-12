using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;

namespace Redis.Stream.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TestController : ControllerBase
    {
        public TestController()
        {
        }

        [HttpGet]
        [Route("{key}")]
        public async Task<IActionResult> Get(
            [FromRoute] string key,
            CancellationToken token)
        {
            var muxer = ConnectionMultiplexer.Connect("localhost");
            var db = muxer.GetDatabase();

            const string streamName = "telemetry";
            const string groupName = "avg";

            // Create the consumer group
            if (!(await db.KeyExistsAsync(streamName)) ||
                (await db.StreamGroupInfoAsync(streamName)).All(x => x.Name != groupName))
            {
                await db.StreamCreateConsumerGroupAsync(streamName, groupName, "0-0", true);
            }

            //Spin up producer task
            var producerTask = Task.Run(async () =>
            {
                var random = new Random();
                while (!token.IsCancellationRequested)
                {
                    await db.StreamAddAsync(streamName,
                        new NameValueEntry[]
                            {
                                new("temp", random.Next(50, 65)),
                                new NameValueEntry("time", DateTimeOffset.Now.ToUnixTimeSeconds())
                            });
                    await Task.Delay(2000);
                }
            });


            return Ok();
        }

        /// <summary>
        /// Parser helper function for reading results
        /// </summary>
        Dictionary<string, string> ParseResult(StreamEntry entry) 
            => entry.Values.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

    }
}