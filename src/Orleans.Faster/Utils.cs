using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;

namespace Orleans.Persistence.Faster
{
    public class Utils
    {
        internal static FasterGrainKey GrainIdAndExtensionAsString(GrainReference grainReference)
        {
            //Kudos for https://github.com/tsibelman for the algorithm. See more at https://github.com/dotnet/orleans/issues/1905.
            string keyExtension;
            FasterGrainKey key;
            if(grainReference.IsPrimaryKeyBasedOnLong())
            {
                key = new FasterGrainKey(grainReference.GetPrimaryKeyLong(out keyExtension), keyExtension);
            }
            else
            {
                key = new FasterGrainKey(grainReference.GetPrimaryKey(out keyExtension), keyExtension);
            }

            return key;
        }

        private static char[] BaseClassExtractionSplitDelimeters { get; } = new[] { '[', ']' };

        /// <summary>
        /// Extracts a base class from a string that is either <see cref="Type.AssemblyQualifiedName"/> or
        /// <see cref="Type.FullName"/> or returns the one given as a parameter if no type is given.
        /// </summary>
        /// <param name="typeName">The base class name to give.</param>
        /// <returns>The extracted base class or the one given as a parameter if it didn't have a generic part.</returns>
        private static string ExtractBaseClass(string typeName)
        {
            var genericPosition = typeName.IndexOf("`", StringComparison.OrdinalIgnoreCase);
            if (genericPosition != -1)
            {
                //The following relies the generic argument list to be in form as described
                //at https://msdn.microsoft.com/en-us/library/w3f99sx1.aspx.
                var split = typeName.Split(BaseClassExtractionSplitDelimeters, StringSplitOptions.RemoveEmptyEntries);
                var stripped = new Queue<string>(split.Where(i => i.Length > 1 && i[0] != ',').Select(WithoutAssemblyVersion));

                return ReformatClassName(stripped);
            }

            return typeName;

            string WithoutAssemblyVersion(string input)
            {
                var asmNameIndex = input.IndexOf(',');
                if (asmNameIndex >= 0)
                {
                    var asmVersionIndex = input.IndexOf(',', asmNameIndex + 1);
                    if (asmVersionIndex >= 0) return input.Substring(0, asmVersionIndex);
                    return input.Substring(0, asmNameIndex);
                }

                return input;
            }

            string ReformatClassName(Queue<string> segments)
            {
                var simpleTypeName = segments.Dequeue();
                var arity = GetGenericArity(simpleTypeName);
                if (arity <= 0) return simpleTypeName;

                var args = new List<string>(arity);
                for (var i = 0; i < arity; i++)
                {
                    args.Add(ReformatClassName(segments));
                }

                return $"{simpleTypeName}[{string.Join(",", args.Select(arg => $"[{arg}]"))}]";
            }

            int GetGenericArity(string input)
            {
                var arityIndex = input.IndexOf("`", StringComparison.OrdinalIgnoreCase);
                if (arityIndex != -1)
                {
                    return int.Parse(input.Substring(arityIndex + 1));
                }

                return 0;
            }
        }
    }
}