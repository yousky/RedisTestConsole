using System;
using System.Collections.Generic;
using System.Text;

namespace RedisTestConsole
{
    public class RedisConnectionStrings
    {
        public string Cluster { get; set; }
        public string Master1 { get; set; }
        public string Master2 { get; set; }
        public string Master3 { get; set; }
        public string Slave1 { get; set; }
        public string Slave2 { get; set; }
        public string Slave3 { get; set; }
        public string Dev { get; set; }
        public string Default { get; set; }
    }
}
