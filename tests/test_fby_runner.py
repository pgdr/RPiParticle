import os.path
from unittest import TestCase
import imp

client_file = os.path.abspath( os.path.join( os.path.dirname(__file__) , "../bin/fby_client"))
main_module = imp.load_source("fby_client" , client_file)


class FbyRunnerTest(TestCase):

    def test_load(self):
        fby_client = main_module.FbyRunner( ["arg1","arg2"] )

    def test_sysinfo(self):
        sys_info = main_module.get_sys_info()
        self.assertTrue('sysname'  in sys_info)
        self.assertTrue('requests' in sys_info)
        self.assertTrue('python'   in sys_info)
