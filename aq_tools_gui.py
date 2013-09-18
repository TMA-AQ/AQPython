#! /usr/bin/python

import wxversion
wxversion.select("2.8")
import wx, wx.html
import sys

import util

class AQFrame(wx.Frame):
	def __init__(self, parent, title):
		super(AQFrame, self).__init__(parent, title=title, size=(1024, 512))
                self.InitUI()
                self.Centre()
		self.Show()

        def InitUI(self):
                # self.Bind(wx.EVT_CLOSE, self.OnClose)

		# menu bar
		menuBar = wx.MenuBar()
		menuFile = wx.Menu()
		menuEdit = wx.Menu()
		menuHelp = wx.Menu()
		# m_exit = menuFile.Append(wx.ID_EXIT, 'E&xit\tAlt-X', 'Close window and exit program.')
		m_exit = wx.MenuItem(menuFile, wx.ID_EXIT, 'Exit', 'Close Window and exit program.')
                menuFile.AppendItem(m_exit)
		menuBar.Append(menuFile, '&File')
		menuBar.Append(menuEdit, '&Edit')
		menuBar.Append(menuHelp, '&Help')
                self.SetMenuBar(menuBar)

		# self.Bind(wx.EVT_MENU, self.OnClose, m_exit)

		# status bar
		self.statusbar = self.CreateStatusBar()

		panel = wx.Panel(self)
		box = wx.BoxSizer(wx.HORIZONTAL)
		
		self.m_aql_ctrl = wx.TextCtrl(panel, wx.ID_ANY, "enter aql query", wx.DefaultPosition, wx.DefaultSize, wx.TE_MULTILINE)
		self.m_sql_ctrl = wx.TextCtrl(panel, wx.ID_ANY, "enter sql query", wx.DefaultPosition, wx.DefaultSize, wx.TE_MULTILINE)
		
		buttonPanel = wx.Panel(panel)
		buttonPanelBox = wx.BoxSizer(wx.VERTICAL)
		m_aql2sql = wx.Button(buttonPanel, wx.ID_ANY, ">>")
		m_sql2aql = wx.Button(buttonPanel, wx.ID_ANY, "<<")
                m_aql2sql.Bind(wx.EVT_BUTTON, self.OnAQL2SQL)
                m_sql2aql.Bind(wx.EVT_BUTTON, self.OnSQL2AQL)
		
		buttonPanelBox.AddStretchSpacer(1)
		buttonPanelBox.Add(m_aql2sql, proportion=0, flag=wx.ALL, border=10)
		buttonPanelBox.AddSpacer(10)
		buttonPanelBox.Add(m_sql2aql, proportion=0, flag=wx.ALL, border=10)
		buttonPanelBox.AddStretchSpacer(1)
		buttonPanel.SetSizer(buttonPanelBox)
                
		box.Add(self.m_aql_ctrl, proportion=1, flag=wx.ALL | wx.EXPAND, border=10)
		box.Add(buttonPanel, proportion=0, flag=wx.ALL | wx.EXPAND, border=10)
		box.Add(self.m_sql_ctrl, proportion=1, flag=wx.ALL | wx.EXPAND, border=10)
		
		panel.SetSizer(box)
		panel.Layout()

	def OnClose(self, event):
		dlg = wx.MessageDialog(self,
			"Do you really want to close this application?",
			"Confirm Exit", wx.OK|wx.CANCEL|wx.ICON_QUESTION)
		result = dlg.ShowModal()
		dlg.Destroy()
		if result == wx.ID_OK:
			self.Destroy()

	def OnAQL2SQL(self, event):
		aql_query = self.m_aql_ctrl.GetValue()
		sql_query = util.aql2sql(aql_query)
		if sql_query != "":
			self.m_sql_ctrl.SetValue(sql_query)

	def OnSQL2AQL(self, event):
		query = self.m_sql_ctrl.GetValue()
		self.m_aql_ctrl.SetValue(query)
		
if __name__ == '__main__':
	app = wx.App(redirect=True)   # Error messages go to popup window
	top = AQFrame(None, "AlgoQuest")
	top.Show()
	app.MainLoop()
