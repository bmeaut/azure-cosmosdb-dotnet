using System;
using System.Collections.Generic;

namespace searchabletodo.Models
{
    public class ItemSearchResults
    {
        public long TotalCount { get; set; }

        public IEnumerable<Item> Items { get; set; }

        public IEnumerable<Tuple<string, long>> TagCounts { get; set; }

        public IEnumerable<Tuple<string, long>> DateCounts { get; set; }
   }
}