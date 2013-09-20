import wxversion
wxversion.select("2.8")
import wx

class AQSettings(wx.Panel):
	def __init__(self, parent):
		super(AQSettings, self).__init__(parent)
		self.InitUI()

	def InitUI(self):
		box = wx.BoxSizer(wx.HORIZONTAL)

		#
		# AlgoQuest options

		box_aq_opts = wx.StaticBox(self, label='AlgoQuest Options')
		box_sizer = wx.StaticBoxSizer(box_aq_opts)

		gbs_aq_opts = wx.GridBagSizer(4, 2)

		aq_db_name_st = wx.StaticText(self, wx.ID_ANY, 'Name')
		aq_db_path_st = wx.StaticText(self, wx.ID_ANY, 'Path')
		aq_engine_st  = wx.StaticText(self, wx.ID_ANY, 'Engine')
		aq_loader_st  = wx.StaticText(self, wx.ID_ANY, 'Loader')

		aq_db_name_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		aq_db_path_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		aq_engine_tc  = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		aq_loader_tc  = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))

		gbs_aq_opts.Add(aq_db_name_st, (0, 0))
		gbs_aq_opts.Add(aq_db_path_st, (1, 0))
		gbs_aq_opts.Add(aq_engine_st,  (2, 0))
		gbs_aq_opts.Add(aq_loader_st,  (3, 0))

		gbs_aq_opts.Add(aq_db_name_tc, (0, 1))
		gbs_aq_opts.Add(aq_db_path_tc, (1, 1))
		gbs_aq_opts.Add(aq_engine_tc,  (2, 1))
		gbs_aq_opts.Add(aq_loader_tc,  (3, 1))

		box_sizer.Add(gbs_aq_opts, proportion=0, flag=wx.ALL, border=0)

		box.Add(box_sizer, proportion=0, flag=wx.ALL, border=10)

		#
		# Database Source options

		box_db_opts = wx.StaticBox(self, label='Database Source Options')
		box_sizer = wx.StaticBoxSizer(box_db_opts, wx.VERTICAL)

		gbs_db_src_opts = wx.GridBagSizer(4, 2)
		
		src_db_host_st = wx.StaticText(self, wx.ID_ANY, 'Host')
		src_db_user_st = wx.StaticText(self, wx.ID_ANY, 'User')
		src_db_pass_st = wx.StaticText(self, wx.ID_ANY, 'Pass')
		src_db_name_st = wx.StaticText(self, wx.ID_ANY, 'Name')
		
		src_db_host_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		src_db_user_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		src_db_pass_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		src_db_name_tc = wx.TextCtrl(self, wx.ID_ANY, '', size=(80, -1))
		
		gbs_db_src_opts.Add(src_db_host_st, (0, 0))
		gbs_db_src_opts.Add(src_db_user_st, (1, 0))
		gbs_db_src_opts.Add(src_db_pass_st, (2, 0))
		gbs_db_src_opts.Add(src_db_name_st, (3, 0))
		
		gbs_db_src_opts.Add(src_db_host_tc, (0, 1))
		gbs_db_src_opts.Add(src_db_user_tc, (1, 1))
		gbs_db_src_opts.Add(src_db_pass_tc, (2, 1))
		gbs_db_src_opts.Add(src_db_name_tc, (3, 1))

		box_sizer.Add(gbs_db_src_opts, proportion=0, flag=wx.EXPAND|wx.ALL, border=0)

		box.Add(box_sizer, proportion=0, flag=wx.ALL, border=10)

		#
		# Database Generation Options

		box_dbg_opts = wx.StaticBox(self, label='Database Generation Options')
		box_sizer = wx.StaticBoxSizer(box_dbg_opts, wx.VERTICAL)
		gen_cb = wx.CheckBox(self, wx.ID_ANY, label='generate database')
	
		min_value_ctrl = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		max_value_ctrl = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		nb_row_ctrl = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		min_value_ctrl.SetRange(1, 50)
		max_value_ctrl.SetRange(1, 50)
		nb_row_ctrl.SetRange(1, 50)
		min_value_ctrl.SetValue(1)
		max_value_ctrl.SetValue(50)
		nb_row_ctrl.SetValue(10)
	
		box_sizer.Add(gen_cb, proportion=0, flag=wx.wx.ALL, border=0)
		box_sizer.Add(min_value_ctrl, proportion=0, flag=wx.wx.ALL, border=0)
		box_sizer.Add(max_value_ctrl, proportion=0, flag=wx.wx.ALL, border=0)
		box_sizer.Add(nb_row_ctrl, proportion=0, flag=wx.wx.ALL, border=0)

		box.Add(box_sizer, proportion=0, flag=wx.ALL, border=10)

		#
		# Commands

		# buttonOK = wx.Button(self, wx.ID_ANY, 'OK')
		# box.Add(buttonOK, proportion=0, flag=wx.RIGHT|wx.BOTTOM, border=10)

		self.SetSizer(box)
