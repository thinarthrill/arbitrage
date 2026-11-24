import unittest
from funding_strategy import Venue, settlements, evaluate
from funding_paper import Policy, PaperPosition, select

class FundingTests(unittest.TestCase):
    def setUp(self):
        self.a=Venue("a",100,100,0.001,3600000,3600000,0)
        self.b=Venue("b",100,100,0.005,7200000,7200000,0)

    def test_unequal_cycles(self):
        r=evaluate(self.a,self.b,0,7200000,1,0)
        self.assertEqual((r["long_cycles"],r["short_cycles"]),(2,1))
        self.assertAlmostEqual(r["funding_usd"],0.3)

    def test_stale(self):
        with self.assertRaises(ValueError):
            settlements(self.a,3600000,7200000)

    def test_ledger(self):
        p=PaperPosition("BTC",self.a,self.b,1,0,Policy(slippage_bps=0))
        p.settle("b",7200000,.005,100)
        p.settle("b",7200000,.005,100)
        p.settle("a",3600000,.001,100)
        self.assertAlmostEqual(p.close(self.a,self.b)["net"],.4)
        with self.assertRaises(ValueError):
            p.close(self.a,self.b)

    def test_select(self):
        p=select("BTC",[self.a,self.b],0,1,Policy(min_net_usd=.01,slippage_bps=0,horizon_ms=7200000))
        self.assertEqual(p.long.name,"a")

if __name__=="__main__":
    unittest.main()
