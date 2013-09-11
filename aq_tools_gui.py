#! /usr/bin/python

import wxversion
wxversion.select("2.8")
import wx, wx.html
import sys

import util

class Frame(wx.Frame):
	def __init__(self, title):
		wx.Frame.__init__(self, None, title=title)
		self.Maximize()
		self.Bind(wx.EVT_CLOSE, self.OnClose)

		# menu bar
		menuBar = wx.MenuBar()
		menu = wx.Menu()
		m_exit = menu.Append(wx.ID_EXIT, "E&xit\tAlt-X", "Close window and exit program.")
		self.Bind(wx.EVT_MENU, self.OnClose, m_exit)
		menuBar.Append(menu, "&File")
		self.SetMenuBar(menuBar)

		# status bar
		self.statusbar = self.CreateStatusBar()

		panel = wx.Panel(self)
		box = wx.BoxSizer(wx.HORIZONTAL)
		
		# m_text = wx.StaticText(panel, -1, "AQL Query")
		# m_text.SetFont(wx.Font(14, wx.SWISS, wx.NORMAL, wx.BOLD))
		# m_text.SetSize(m_text.GetBestSize())
		# box.Add(m_text, 0, wx.ALL, 10)

		self.m_aql_ctrl = wx.TextCtrl(panel, wx.ID_ANY, "enter aql query", wx.DefaultPosition, wx.DefaultSize, wx.TE_MULTILINE)
		self.m_sql_ctrl = wx.TextCtrl(panel, wx.ID_ANY, "enter sql query", wx.DefaultPosition, wx.DefaultSize, wx.TE_MULTILINE)
		
		buttonPanel = wx.Panel(panel)
		buttonPanelBox = wx.BoxSizer(wx.VERTICAL)
		
		m_aql2sql = wx.Button(buttonPanel, wx.ID_ANY, ">>")
		m_aql2sql.Bind(wx.EVT_BUTTON, self.OnAQL2SQL)
		m_sql2aql = wx.Button(buttonPanel, wx.ID_ANY, "<<")
		m_sql2aql.Bind(wx.EVT_BUTTON, self.OnSQL2AQL)
		
		buttonPanelBox.AddStretchSpacer(1)
		buttonPanelBox.Add(m_aql2sql, 0, wx.ALL, -1)
		buttonPanelBox.AddSpacer(10)
		buttonPanelBox.Add(m_sql2aql, 0, wx.ALL, -1)
		buttonPanelBox.AddStretchSpacer(1)
		buttonPanel.SetSizer(buttonPanelBox)

		box.Add(self.m_aql_ctrl, 10, wx.ALL | wx.EXPAND, -1)
		box.Add(buttonPanel, 1, wx.ALL | wx.EXPAND, -1)
		box.Add(self.m_sql_ctrl, 10, wx.ALL | wx.EXPAND, -1)
		
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
	top = Frame("AlgoQuest")
	top.Show()
	app.MainLoop()
