using log4net;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace RedisTestConsole
{
    public class RedisService : IDisposable
    {
        public ILog _logger = LogManager.GetLogger(typeof(RedisService));

        ConnectionMultiplexer _redis;
        ISubscriber _subscriber;
        ISubscriber _publisher;

        string _redisConnectionConfiguration;

        int _redisId;

        public RedisService(int redisId, string redisConnectionConfiguration)
        {
            this._redisId = redisId;
            this._redisConnectionConfiguration = redisConnectionConfiguration;
        }

        public async Task SetAsync()
        {
            try
            {
                _redis = await ConnectionMultiplexer.ConnectAsync(_redisConnectionConfiguration);
                _redis.ConnectionFailed += (sender, args) =>
                {
                    _logger.Warn($"[{_redisId}]Connection Failed: args.EndPoint={args.EndPoint}, args.ConnectionType={args.ConnectionType}, args.FailureType={args.FailureType}", args.Exception);
                };
                _redis.ConnectionRestored += (sender, args) =>
                {
                    _logger.Warn($"[{_redisId}]Connection Restored: args.EndPoint={args.EndPoint}, args.ConnectionType={args.ConnectionType}, args.FailureType={args.FailureType}", args.Exception);
                };
                _redis.ErrorMessage += (sender, args) =>
                {
                    _logger.Warn($"[{_redisId}]ErrorMessage : args.EndPoint={args.EndPoint}, args.Message={args.Message}");
                };

                _subscriber = _redis.GetSubscriber();
                _publisher = _redis.GetSubscriber();
            }
            catch (Exception ex)
            {
                _logger.Error($"[{_redisId}]redis not connected", ex);
                throw;
            }
        }

        public async Task PSubscribe(string pattern, int milis = 1)
        {
            RedisChannel subsPatternChannel = new RedisChannel(pattern, RedisChannel.PatternMode.Auto);

            await _subscriber.SubscribeAsync(subsPatternChannel, async (channel, message) =>
            {
                string workId = Guid.NewGuid().ToString();
                _logger.Info($"[{_redisId}]On {channel}[{workId}] : {message}");
                await Task.Delay(milis);
                _logger.Info($"[{_redisId}]On {channel}[{workId}] : {message} Processed");
            });
            _logger.Info($"[{_redisId}]PSubscribe : " + pattern);
        }

        public async Task PUnubscribe(string pattern)
        {
            RedisChannel subsPatternChannel = new RedisChannel(pattern, RedisChannel.PatternMode.Auto);
            await _subscriber.UnsubscribeAsync(subsPatternChannel);
            _logger.Info($"[{_redisId}]PUnubscribe : " + pattern);
        }

        public async Task PSubscribeWithTwoWayHandShakeAsync(string pattern, int milis = 1)
        {
            RedisChannel subsPatternChannel = new RedisChannel(pattern, RedisChannel.PatternMode.Auto);

            Regex replyToRegex = new Regex(@"\/replyTo\/([a-zA-Z0-9-]+)", RegexOptions.IgnoreCase);


            await _subscriber.SubscribeAsync(subsPatternChannel, async (channel, message) =>
            {
                Match replyMatch = replyToRegex.Match(channel);
                if (replyMatch.Success)
                {
                    string replyTo = $"/replyTo/{replyMatch.Groups[1].Value}";
                    RedisChannel replyChannel = new RedisChannel(replyTo, RedisChannel.PatternMode.Auto);
                    long redisCnt = _publisher.Publish(replyChannel, 1);
                    _logger.Info($"[{_redisId}]PublishAsync {replyChannel}[redisCnt: {redisCnt}] : reply 1");
                }

                string workId = Guid.NewGuid().ToString();
                _logger.Info($"[{_redisId}]On {channel}[{workId}] : {message}");
                await Task.Delay(milis);
                _logger.Info($"[{_redisId}]On {channel}[{workId}] : {message} Processed");
            });
            _logger.Info($"[{_redisId}]PSubscribe : " + pattern);
        }

        public async Task<long> PublishWithTwoWayHandShakeAsync(string channel, string message)
        {
            //응답을 받기 위한 전 처리부
            string replyTo = $"/replyTo/{Guid.NewGuid().ToString()}";
            channel = channel + replyTo;
            var waiter = new BlockingCollection<long>();
            RedisChannel subsPatternChannel = new RedisChannel(replyTo, RedisChannel.PatternMode.Auto); //구독
            var tempSubscriber = _redis.GetSubscriber();
            await tempSubscriber.SubscribeAsync(subsPatternChannel, (subsChannel, subsMessage) =>
            {
                string workId = Guid.NewGuid().ToString();
                _logger.Info($"[{_redisId}]On {subsChannel}[{workId}] : {subsMessage}");

                if (subsMessage.HasValue)
                {
                    waiter.Add((long)subsMessage);
                }
            });
            _logger.Info($"[{_redisId}] ReplyTo Subscribe : " + replyTo);


            //실제 Publish 하는 곳
            RedisChannel redisChannel = new RedisChannel(channel, RedisChannel.PatternMode.Auto);
            long redisCnt = _publisher.Publish(redisChannel, message);
            _logger.Info($"[{_redisId}]PublishAsync {channel}[redisCnt: {redisCnt}] : {message}");


            //Publish한 것에 대한 받았다는 확인 체크 부
            var success = waiter.TryTake(out var result, 500);

            await tempSubscriber.UnsubscribeAsync(subsPatternChannel); //구독 취소
            _logger.Info($"[{_redisId}] ReplyTo Unubscribe : " + replyTo);
            tempSubscriber = null;


            if (success)
            {
                _logger.Info($"[{_redisId}]PublishAsync {channel}[TwoWayHandShake result: {result}] : {message}");
                return result;
            }

            _logger.Warn($"[{_redisId}]PublishAsync {channel}[TwoWayHandShake result: fail] : {message}");
            return -1;
        }

        public async Task<long> PublishWithTwoWayHandShakeAsync(string channel, string message, int milis = 1, int repeats = 0)
        {
            int runCnt = 0;
            long redisTotCnt = 0;
            do
            {
                runCnt++;
                long redisCnt = await PublishWithTwoWayHandShakeAsync(channel, message);
                _logger.Info($"[{_redisId}]PublishWithTwoWayHandShakeAsync {channel}[redisCnt: {redisCnt}] : {message}");
                await Task.Delay(milis);
                redisTotCnt = redisTotCnt + redisCnt;
            } while (runCnt < repeats);

            return redisTotCnt;
        }


        public async Task<long> PublishAsync(string channel, string message, int milis = 1, int repeats = 0)
        {
            RedisChannel redisChannel = new RedisChannel(channel, RedisChannel.PatternMode.Auto);

            int runCnt = 0;
            long redisTotCnt = 0;
            do
            {
                runCnt++;
                long redisCnt = _publisher.Publish(redisChannel, message);
                _logger.Info($"[{_redisId}]PublishAsync {channel}[redisCnt: {redisCnt}] : {message}");
                await Task.Delay(milis);
                redisTotCnt = redisTotCnt + redisCnt;
            } while (runCnt < repeats);

            return redisTotCnt;
        }

        #region IDisposable Support
        private bool disposedValue = false; // 중복 호출을 검색하려면

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (_redis != null)
                    {
                        _redis.Dispose();
                    }
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
