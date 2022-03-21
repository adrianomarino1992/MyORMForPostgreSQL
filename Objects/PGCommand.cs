using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyORM.Enums;
using MyORM.Interfaces;

namespace MyORMForPostgreSQL.Objects
{
    public class PGCommand : ICommand
    {
        private string? _sql;
        public ProviderType ProviderType { get => ProviderType.POSTGRESQL; set => _=value; }

        public string Sql()
        {
            return _sql ?? String.Empty;
        }

        public PGCommand(string sql)
        {
            _sql = sql;
        }
    }
}
