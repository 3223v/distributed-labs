 import sys, os
  sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
  from mini_raft_kv.app import main
  import asyncio
  asyncio.run(main())