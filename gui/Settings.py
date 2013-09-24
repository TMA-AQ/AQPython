import wxversion
wxversion.select("2.8")
import wx
import ConfigParser

#
#
def __print_cfg__(cfg):
	for s in cfg.sections():
		print s
		for k, v in cfg.items(s):
			print '  ', k, v

#
#		
def __copy_cfg__(cfg):
	clone = ConfigParser.SafeConfigParser()
	for s in cfg.sections():
		clone.add_section(s)
		for k, v in cfg.items(s):
			clone.set(s, k, v) 
	return clone
	
#
#
class AQSettings(wx.Panel):
	
	#
	#
	def __init__(self, parent, settings):
		super(AQSettings, self).__init__(parent)
		self.settings = __copy_cfg__(settings)
		self.__initialSettings__ = settings
		self.InitUI()

	#
	#
	def InitUI(self):
		box = wx.BoxSizer(wx.VERTICAL)

		#
		# AlgoQuest options

		box_aq_opts = wx.StaticBox(self, label='AlgoQuest Options')
		box_sizer = wx.StaticBoxSizer(box_aq_opts)

		gbs_aq_opts = wx.GridBagSizer(4, 2)

		aq_db_name_st = wx.StaticText(self, wx.ID_ANY, 'Name')
		aq_db_path_st = wx.StaticText(self, wx.ID_ANY, 'Path')
		aq_engine_st  = wx.StaticText(self, wx.ID_ANY, 'Engine')
		aq_loader_st  = wx.StaticText(self, wx.ID_ANY, 'Loader')

		self.aq_db_name_tc = wx.TextCtrl(self, wx.ID_ANY)
		self.aq_db_path_tc = wx.TextCtrl(self, wx.ID_ANY)
		self.aq_engine_tc  = wx.TextCtrl(self, wx.ID_ANY)
		self.aq_loader_tc  = wx.TextCtrl(self, wx.ID_ANY)

		gbs_aq_opts.Add(aq_db_name_st, (0, 0))
		gbs_aq_opts.Add(aq_db_path_st, (1, 0))
		gbs_aq_opts.Add(aq_engine_st,  (2, 0))
		gbs_aq_opts.Add(aq_loader_st,  (3, 0))

		gbs_aq_opts.Add(self.aq_db_name_tc, (0, 1), flag=wx.EXPAND)
		gbs_aq_opts.Add(self.aq_db_path_tc, (1, 1), flag=wx.EXPAND)
		gbs_aq_opts.Add(self.aq_engine_tc,  (2, 1), flag=wx.EXPAND)
		gbs_aq_opts.Add(self.aq_loader_tc,  (3, 1), flag=wx.EXPAND)

		gbs_aq_opts.AddGrowableCol(1)
		
		box_sizer.Add(gbs_aq_opts, proportion=1, border=10)

		box.Add(box_sizer, proportion=0, flag=wx.ALL|wx.EXPAND, border=10)

		#
		# Database Source options

		box_db_opts = wx.StaticBox(self, label='Database Source Options')
		box_sizer = wx.StaticBoxSizer(box_db_opts)

		gbs_db_src_opts = wx.GridBagSizer(4, 2)
		
		src_db_host_st = wx.StaticText(self, wx.ID_ANY, 'Host')
		src_db_user_st = wx.StaticText(self, wx.ID_ANY, 'User')
		src_db_pass_st = wx.StaticText(self, wx.ID_ANY, 'Pass')
		src_db_name_st = wx.StaticText(self, wx.ID_ANY, 'Name')
		
		self.src_db_host_tc = wx.TextCtrl(self, wx.ID_ANY)
		self.src_db_user_tc = wx.TextCtrl(self, wx.ID_ANY)
		self.src_db_pass_tc = wx.TextCtrl(self, wx.ID_ANY)
		self.src_db_name_tc = wx.TextCtrl(self, wx.ID_ANY)
		
		gbs_db_src_opts.Add(src_db_host_st, (0, 0))
		gbs_db_src_opts.Add(src_db_user_st, (1, 0))
		gbs_db_src_opts.Add(src_db_pass_st, (2, 0))
		gbs_db_src_opts.Add(src_db_name_st, (3, 0))
		
		gbs_db_src_opts.Add(self.src_db_host_tc, (0, 1), flag=wx.EXPAND)
		gbs_db_src_opts.Add(self.src_db_user_tc, (1, 1), flag=wx.EXPAND)
		gbs_db_src_opts.Add(self.src_db_pass_tc, (2, 1), flag=wx.EXPAND)
		gbs_db_src_opts.Add(self.src_db_name_tc, (3, 1), flag=wx.EXPAND)

		gbs_db_src_opts.AddGrowableCol(1)
		
		box_sizer.Add(gbs_db_src_opts, proportion=1, border=10)

		box.Add(box_sizer, proportion=0, flag=wx.ALL|wx.EXPAND, border=10)

		#
		# Database Generation Options

		box_dbg_opts = wx.StaticBox(self, label='Database Generation Options')
		box_sizer = wx.StaticBoxSizer(box_dbg_opts, wx.VERTICAL)
		
		self.gen_cb = wx.CheckBox(self, wx.ID_ANY, label='generate database')
		self.min_value_ctrl = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		self.max_value_ctrl = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		self.nb_row_ctrl    = wx.SpinCtrl(self, wx.ID_ANY, '', (30, 50))
		
		self.min_value_ctrl.SetRange(1, 50)
		self.max_value_ctrl.SetRange(1, 50)
		self.nb_row_ctrl.SetRange(1, 50)
		self.min_value_ctrl.SetValue(1)
		self.max_value_ctrl.SetValue(50)
		self.nb_row_ctrl.SetValue(10)
	
		box_sizer.Add(self.gen_cb, proportion=0, flag=wx.wx.ALL, border=0)
		box_sizer.Add(self.min_value_ctrl, proportion=0, flag=wx.wx.ALL|wx.EXPAND, border=0)
		box_sizer.Add(self.max_value_ctrl, proportion=0, flag=wx.wx.ALL|wx.EXPAND, border=0)
		box_sizer.Add(self.nb_row_ctrl, proportion=0, flag=wx.wx.ALL|wx.EXPAND, border=0)

		box.Add(box_sizer, proportion=0, flag=wx.ALL|wx.EXPAND, border=10)

		#
		# Commands

		bPanel = wx.Panel(self)
		bbox = wx.BoxSizer(wx.HORIZONTAL)
		
		buttonReset = wx.Button(bPanel, wx.ID_ANY, 'Restore')
		bbox.Add(buttonReset, proportion=0, flag=wx.RIGHT|wx.ALIGN_RIGHT|wx.ALIGN_BOTTOM, border=10)
		self.Bind(wx.EVT_BUTTON, self.__OnRestore__, buttonReset)

		buttonWrite = wx.Button(bPanel, wx.ID_ANY, 'Save')
		bbox.Add(buttonWrite, proportion=0, flag=wx.RIGHT|wx.ALIGN_RIGHT|wx.ALIGN_BOTTOM, border=10)
		self.Bind(wx.EVT_BUTTON, self.__OnSave__, buttonWrite)

		buttonOK = wx.Button(bPanel, wx.ID_ANY, 'OK')
		bbox.Add(buttonOK, proportion=0, flag=wx.RIGHT|wx.ALIGN_RIGHT|wx.ALIGN_BOTTOM, border=10)
		self.Bind(wx.EVT_BUTTON, self.__OnValid__, buttonOK)

		bPanel.SetSizer(bbox)
		box.Add(bPanel, proportion=0, flag=wx.ALL|wx.ALIGN_RIGHT|wx.ALIGN_BOTTOM, border=0)
		
		self.SetSizer(box)
		self.Layout()
		
		self.__initValues__()
		
	#
	#
	def __initValues__(self):
		
		section = 'AlgoQuest Options'
		self.aq_db_name_tc.SetValue(self.settings.get(section, 'aq-db-name'))
		self.aq_db_path_tc.SetValue(self.settings.get(section, 'aq-db-path'))
		self.aq_engine_tc.SetValue(self.settings.get(section, 'aq-engine'))
		self.aq_loader_tc.SetValue(self.settings.get(section, 'aq-loader'))
		
		section = 'Source Database Options'
		self.src_db_host_tc.SetValue(self.settings.get(section, 'db-host'))
		self.src_db_user_tc.SetValue(self.settings.get(section, 'db-user'))
		self.src_db_pass_tc.SetValue(self.settings.get(section, 'db-pass'))
		self.src_db_name_tc.SetValue(self.settings.get(section, 'db-name'))
		
		section = 'Database Generation Options'
		self.min_value_ctrl.SetValue(self.settings.getint(section, 'min-value'))
		self.max_value_ctrl.SetValue(self.settings.getint(section, 'max-value'))
		self.nb_row_ctrl.SetValue(   self.settings.getint(section, 'nb-rows'))
		
	#
	#
	def __OnRestore__(self, event):
		self.settings = __copy_cfg__(self.__initialSettings__)
		self.__initValues__()

	#
	#
	def __OnSave__(self, event):
		__print_cfg__(self.settings)

	#
	#
	def __OnValid__(self, event):
		
		section = 'AlgoQuest Options'
		self.settings.set(section, 'aq-db-name', self.aq_db_name_tc.GetValue())
		self.settings.set(section, 'aq-db-path', self.aq_db_path_tc.GetValue())
		self.settings.set(section, 'aq-engine', self.aq_engine_tc.GetValue())
		self.settings.set(section, 'aq-loader', self.aq_loader_tc.GetValue())
		
		section = 'Source Database Options'
		self.settings.set(section, 'db-host', self.src_db_host_tc.GetValue())
		self.settings.set(section, 'db-user', self.src_db_user_tc.GetValue())
		self.settings.set(section, 'db-pass', self.src_db_pass_tc.GetValue())
		self.settings.set(section, 'db-name', self.src_db_name_tc.GetValue())
		
		section = 'Database Generation Options'
		self.settings.set(section, 'min_value', str(self.min_value_ctrl.GetValue()))
		self.settings.set(section, 'max_value', str(self.max_value_ctrl.GetValue()))
		self.settings.set(section, 'nb_rows'  , str(self.nb_row_ctrl.GetValue()))
		
		print ''
		__print_cfg__(self.settings)
		print ''
		__print_cfg__(self.__initialSettings__)
