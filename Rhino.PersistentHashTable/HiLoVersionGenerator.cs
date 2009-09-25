using System;
using System.Runtime.CompilerServices;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable
{
	public class HiLoVersionGenerator : IVersionGenerator
	{
		private readonly JET_INSTANCE instance;
		private readonly int capacity;
		private readonly string database;
		private volatile int currentLo;
		private volatile int currentHi;

		public HiLoVersionGenerator(JET_INSTANCE instance, int capacity, string database)
		{
			this.instance = instance;
			this.capacity = capacity;
			this.database = database;
			currentHi = GenerateNextHi();
			currentLo = 0;
		}

		/// <summary>
		/// Only one thread may execute this at a given time.
		/// Note that this forces a NEW session and transaction, and doesn't use the existing one.
		/// </summary>
		/// <returns></returns>
		private int GenerateNextHi()
		{
			int value;
			using (var anotherSession = new Session(instance))
			using (var anotherTx = new Transaction(anotherSession))
			{
				JET_DBID dbid;
				Api.JetOpenDatabase(anotherSession, database, "", out dbid, OpenDatabaseGrbit.None);

				try
				{
					using (var nextHi = new Table(anotherSession, dbid, "next_hi", OpenTableGrbit.DenyRead |
																						  OpenTableGrbit.NoCache |
																						  OpenTableGrbit.Updatable))
					{
						if (Api.TryMoveFirst(anotherSession, nextHi) == false)
							throw new InvalidOperationException(
								"Could not find existing next_hi value, it is likely that the PHT is corrupted");

						var dictionary = Api.GetColumnDictionary(anotherSession, nextHi);

						using (var update = new Update(anotherSession, nextHi, JET_prep.Replace))
						{
							value = (int)Api.RetrieveColumnAsInt32(anotherSession, nextHi, dictionary["val"]);
							value += 1;
							Api.SetColumn(anotherSession, nextHi, dictionary["val"], value);

							update.Save();
						}
					}

				}
				finally
				{
					Api.JetCloseDatabase(anotherSession, dbid, CloseDatabaseGrbit.None);
				}
				anotherTx.Commit(CommitTransactionGrbit.None);
			}

			return value;
		}
		/// <summary>
		/// The formula is: (Hi-1)*Capacity+(++Lo) 
		/// Full details about it can be found here:
		/// http://devlicio.us/blogs/tuna_toksoz/archive/2009/05/18/id-generation-for-db4o.aspx
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public int GenerateNextVersion()
		{
			if(currentLo>=capacity)
			{
				currentHi = GenerateNextHi();
				currentLo = 0;
			}

			return (currentHi - 1) * capacity + (++currentLo);  
		}
	}
}