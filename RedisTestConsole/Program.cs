using log4net;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace RedisTestConsole
{
    class Program
    {
        static ILog _logger;

        static int _lastRedisId = 0;
        static string _redisConnectionConfiguration;

        static Dictionary<int, RedisService> _dicRedisService = new Dictionary<int, RedisService>();
        static RedisService _pubRedisService;
        static RedisService _subRedisService;

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }


        static async Task MainAsync(string[] args)
        {
            //log4net 환경 처리
            InitLog4Net();

            IntConfig(args);

            try
            {
                _pubRedisService = await NewRedisService(true);
                _subRedisService = await NewRedisService(true);

                Console.WriteLine("Your input (q to quit)");
                Console.WriteLine("");
                Console.WriteLine("ex) psub \"pattern/*\" d1000 s2 c");
                Console.WriteLine("Subscribe a pattern/* with 2 subscriber and delay 1000ms, certenly(2way handshake).");
                Console.WriteLine("1 always corresponds to the same service, and in the case of 2 or more, it becomes a increment service from 2.");
                Console.WriteLine("");
                Console.WriteLine("ex) pub \"pattern/aaa\" \"message\" d1 w10 c");
                Console.WriteLine("Publish a pattern/aaa with 10 repeat and delay 1ms, certenly(2way handshake)");
                Console.WriteLine("");
                Console.WriteLine("ex) punsub \"pattern/*\" s2");
                Console.WriteLine("UnSubscribe a pattern/* with 2 subscriber");
                Console.WriteLine("1 always corresponds to the same service, and in the case of 2 or more, it becomes a increment service from 2.");
                Console.WriteLine("");

                string input;
                do
                {
                    input = Console.ReadLine();

                    Match match;
                    if (UsePublish(input, out match))
                    {
                        Console.WriteLine($"UsePublish : {input}");
                        await PublishAsync(match);
                    }
                    else if (UsePSubs(input, out match))
                    {
                        Console.WriteLine($"UsePSubs : {input}");
                        await PSubsAsync(match);
                    }
                    else if (UsePUnSubs(input, out match))
                    {
                        Console.WriteLine($"UsePUnSubs : {input}");
                        await PUnSubsAsync(match);
                    }
                    //else if (input.ToLower() == "d1")
                    //{
                    //    Console.WriteLine($"DELETE RedisService : {input}");
                    //    DisposeRedisService(true, _subRedisService);
                    //}
                    else
                    {
                        Console.WriteLine($"Unknown Command : {input}");
                    }

                } while (string.IsNullOrWhiteSpace(input) || input.ToLower() != "q");

                Console.WriteLine("bye bye");
            }
            catch (Exception ex)
            {
                _logger.Error("Error MainAsync", ex);
            }

        }

        private static void IntConfig(string[] args)
        {
            if (!File.Exists("appsettings.json"))
            {
                _logger.Fatal(@"The appsettings.json file does not exist." + Environment.NewLine 
                    + @"Please create appsettings.json by referring to the appsettings.sample.json file.");
                throw new FileNotFoundException(@"The appsettings.json file does not exist.", "appsettings.json");
            }

            IConfiguration Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            var redisConnectionStrings = Configuration.GetSection("RedisConnectionStrings").Get<RedisConnectionStrings>();

            Console.WriteLine(@"Input parameters are m1, m2, m3, s1, s2, s3, and dev." + Environment.NewLine + 
                @"Determine the master1 ~ 3, slave1 ~ 3, and dev redis environments, respectively." + Environment.NewLine +
                @"If they don't match, use the default." + Environment.NewLine + Environment.NewLine);

            if (args.Length > 0)
            {
                switch (args[0].ToLower())
                {
                    case "m1":
                        _redisConnectionConfiguration = redisConnectionStrings.Master1;
                        _logger.Info("Using Master1 RedisConnectionString");
                        break;
                    case "m2":
                        _redisConnectionConfiguration = redisConnectionStrings.Master2;
                        _logger.Info("Using Master2 RedisConnectionString");
                        break;
                    case "m3":
                        _redisConnectionConfiguration = redisConnectionStrings.Master3;
                        _logger.Info("Using Master3 RedisConnectionString");
                        break;
                    case "s1":
                        _redisConnectionConfiguration = redisConnectionStrings.Slave1;
                        _logger.Info("Using Slave1 RedisConnectionString");
                        break;
                    case "s2":
                        _redisConnectionConfiguration = redisConnectionStrings.Slave2;
                        _logger.Info("Using Slave2 RedisConnectionString");
                        break;
                    case "s3":
                        _redisConnectionConfiguration = redisConnectionStrings.Slave3;
                        _logger.Info("Using Slave3 RedisConnectionString");
                        break;
                    case "dev":
                        _redisConnectionConfiguration = redisConnectionStrings.Dev;
                        _logger.Info("Using Dev RedisConnectionString");
                        break;
                    default:
                        _redisConnectionConfiguration = redisConnectionStrings.Default;
                        _logger.Info("Using Default RedisConnectionString");
                        break;
                }
            }
            else
            {
                _redisConnectionConfiguration = redisConnectionStrings.Default;
                _logger.Info("Using Default RedisConnectionString");
            }
        }

        private static void InitLog4Net()
        {
            XmlDocument log4netConfig = new XmlDocument();
            log4netConfig.Load(File.OpenRead("log4net.config"));
            var repo = log4net.LogManager.CreateRepository(Assembly.GetEntryAssembly(),
                       typeof(log4net.Repository.Hierarchy.Hierarchy));
            log4net.Config.XmlConfigurator.Configure(repo, log4netConfig["log4net"]);

            _logger = LogManager.GetLogger(typeof(Program));
        }

        private static async Task<RedisService> NewRedisService(bool useDic)
        {
            RedisService redisService = new RedisService(_lastRedisId++, _redisConnectionConfiguration);
            if (useDic)
            {
                _dicRedisService.Add(_lastRedisId, _pubRedisService);
            }

            await redisService.SetAsync();
            return redisService;
        }

        private static void DisposeRedisService(bool useDic, RedisService redisService)
        {
            if (useDic)
            {
                _dicRedisService.Remove(redisService._redisId);
            }

            redisService.Dispose();
        }

        protected static bool UsePUnSubs(string input, out Match match)
        {
            Regex regex = new Regex("^punsub \"(.+)\"( +s(\\d+))?$", RegexOptions.IgnoreCase);  //^punsub "(.+)"( +s(\d+))?$     punsub "pattern" s5

            match = regex.Match(input);
            if (match.Success)
            {
                return true;
            }

            return false;
        }

        protected static async Task PUnSubsAsync(Match match)
        {
            //punsub "pattern" s5

            string pattern = match.Groups[1].Value;
            string strServiceCnt = match.Groups[3].Value;

            int serviceCnt = string.IsNullOrWhiteSpace(strServiceCnt) ? 1 : int.Parse(strServiceCnt);

            for (int i = 0; i < serviceCnt; i++)
            {
                RedisService redisService;
                if (i == 0)
                {
                    redisService = _subRedisService;
                }
                else
                {
                    redisService = await NewRedisService(true);
                }

                await redisService.PUnSubscribe(pattern);
            }
        }

        protected static bool UsePSubs(string input, out Match match)
        {
            Regex regex = new Regex("^psub \"(.+)\"( +d(\\d+))?( +s(\\d+))?( +c)?$", RegexOptions.IgnoreCase);  //^psub "(.+)"( +d(\d+))?( +s(\d+))?( +c)?$     psub "pattern" d1 s5 c

            match = regex.Match(input);
            if (match.Success)
            {
                return true;
            }

            return false;
        }

        protected static async Task PSubsAsync(Match match)
        {
            //psub "pattern" d1 t5 c

            string pattern = match.Groups[1].Value;
            string strDelayMilis = match.Groups[3].Value;
            string strServiceCnt = match.Groups[5].Value;
            string strCertenly = match.Groups[6].Value;

            int delayMilis = string.IsNullOrWhiteSpace(strDelayMilis) ? 1 : int.Parse(strDelayMilis);
            int serviceCnt = string.IsNullOrWhiteSpace(strServiceCnt) ? 1 : int.Parse(strServiceCnt);

            for (int i = 0; i < serviceCnt; i++)
            {
                RedisService redisService;
                if (i == 0)
                {
                    redisService = _subRedisService;
                }
                else
                {
                    redisService = await NewRedisService(true);
                }

                if (string.IsNullOrWhiteSpace(strCertenly))
                {
                    await redisService.PSubscribe(pattern, delayMilis);
                }
                else
                {
                    await redisService.PSubscribeWithTwoWayHandShakeAsync(pattern, delayMilis);
                }
            }
        }

        protected static bool UsePublish(string input, out Match match)
        {
            Regex regex = new Regex("^pub \"(.+)\" \"(.+)\"( +d(\\d+))?( +w(\\d+))?( +c)?$", RegexOptions.IgnoreCase);  //^pub "(.+)" "(.+)"( +d(\d+))?( +w(\d+))?( +c)?$     pub "channel" "message" d1 w30 c

            match = regex.Match(input);
            if (match.Success)
            {
                return true;
            }

            return false;
        }

        protected static Task<long> PublishAsync(Match match)
        {
            //pub "channel" "message" d1 w30 c

            string channel = match.Groups[1].Value;
            string message = match.Groups[2].Value;
            string strDelayMilis = match.Groups[4].Value;
            string strRepeatCnt = match.Groups[6].Value;
            string strCertenly = match.Groups[7].Value;

            int delayMilis = string.IsNullOrWhiteSpace(strDelayMilis) ? 1 : int.Parse(strDelayMilis);
            int repeatCnt = string.IsNullOrWhiteSpace(strRepeatCnt) ? 1 : int.Parse(strRepeatCnt);

            if (string.IsNullOrWhiteSpace(strCertenly))
            {
                return _pubRedisService.PublishAsync(channel, message, delayMilis, repeatCnt);
            }
            else
            {
                return _pubRedisService.PublishWithTwoWayHandShakeAsync(channel, message, delayMilis, repeatCnt);
            }
        }

        protected static Task<long> RPCAsync(Match match)
        {
            //pub "channel" "message" d1 w30

            string channel = match.Groups[1].Value;
            string message = match.Groups[2].Value;
            string strDelayMilis = match.Groups[4].Value;
            string strRepeatCnt = match.Groups[6].Value;

            int delayMilis = string.IsNullOrWhiteSpace(strDelayMilis) ? 1 : int.Parse(strDelayMilis);
            int repeatCnt = string.IsNullOrWhiteSpace(strRepeatCnt) ? 1 : int.Parse(strRepeatCnt);

            return _pubRedisService.PublishAsync(channel, message, delayMilis, repeatCnt);
        }

    }
}
