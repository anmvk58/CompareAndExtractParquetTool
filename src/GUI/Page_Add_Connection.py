import tkinter as tk
from tkinter import *
import json
from src.common.io_utils import IOUtils
from src.common.default_var import DefaultVar
import src.common.encryptor as encryptor
from common.config_reader import Config_reader

config_reader = Config_reader()

class AddConnection:
    def __init__(self, parent):
        top = self.top = tk.Toplevel(parent)
        x = parent.winfo_x()
        y = parent.winfo_y()
        self.top.geometry("+%d+%d" % (x + 250, y + 200))
        self.top.geometry('500x285')
        self.top.maxsize(500, 285)
        self.top.minsize(500, 285)
        self.top.title('Add Connection')
        self.top.attributes('-toolwindow', True)
        self.top.attributes('-topmost', True)
        self.top.focus()

        # Src_name
        label_src_name = Label(self.top, text="Source Name", font=("Calibri", 12))
        label_src_name.grid(sticky="w", row=0, column=0, padx=(20, 0),pady=(20, 0))

        self.text_src_name = Entry(self.top, font=("Calibri", 12), width=45)
        self.text_src_name.grid(columnspan=3, row=0, column=1, padx=(0, 15),pady=(20, 0))

        # Engine
        label_engine = Label(self.top, text="Engine", font=("Calibri", 12))
        label_engine.grid(sticky="w", row=1, column=0, padx=(20, 0), pady=2)

        list_engine_db = ["ORACLE", "MSSQL", "DB2"]
        self.engine = StringVar(self.top)
        self.engine.set("Select Engine")
        self.engine.trace("w", self.option_changed)
        engine_drop_list = OptionMenu(self.top, self.engine, *list_engine_db)
        engine_drop_list.config(width=27)
        engine_drop_list.grid(sticky="w", columnspan=3, row=1, column=1, padx=(0, 15), pady=2)

        # Host
        label_host = Label(self.top, text="Host", font=("Calibri", 12))
        label_host.grid(sticky="w", row=2, column=0, padx=(20, 0), pady=2)

        self.text_host = Entry(self.top, font=("Calibri", 12), width=25)
        self.text_host.grid(sticky="w", row=2, column=1, padx=(0, 15), pady=2)

        # Port
        label_port = Label(self.top, text="Port", font=("Calibri", 12))
        label_port.grid(sticky="e", row=2, column=2, padx=(15, 0), pady=2)

        self.text_port = Entry(self.top, font=("Calibri", 12), width=10)
        self.text_port.grid(sticky="e", row=2, column=3, padx=(10, 15), pady=2)

        # Service
        self.label_service_name_value = tk.StringVar()
        self.label_service_name = Label(self.top, textvariable=self.label_service_name_value, font=("Calibri", 12))
        # .grid(sticky="w", row=5, column=0, padx=(15, 0), pady=2)
        self.text_service_name = Entry(self.top, font=("Calibri", 12), width=45)
        # .grid(columnspan=3, row=5, column=1, padx=(0, 15), pady=2)

        # Username
        label_username = Label(self.top, text="Username", font=("Calibri", 12))
        label_username.grid(sticky="w", row=4, column=0, padx=(20, 0), pady=2)

        self.text_username = Entry(self.top, font=("Calibri", 12), width=45)
        self.text_username.grid(columnspan=3, row=4, column=1, padx=(0, 15), pady=2)

        # Password
        label_pass = Label(self.top, text="Password", font=("Calibri", 12))
        label_pass.grid(sticky="w", row=5, column=0, padx=(20, 0), pady=2)

        self.text_pass = Entry(self.top, font=("Calibri", 12), width=45, show="•")
        self.text_pass.grid(columnspan=3, row=5, column=1, padx=(0, 15), pady=2)

        # Schema
        label_schema = Label(self.top, text="Schema", font=("Calibri", 12))
        label_schema.grid(sticky="w", row=6, column=0, padx=(20, 0), pady=2)

        self.text_schema = Entry(self.top, font=("Calibri", 12), width=45)
        self.text_schema.grid(columnspan=3, row=6, column=1, padx=(0, 15), pady=2)

        # Label Notification
        self.label_noti = Label(self.top, text="", fg="red", font=("Calibri", 12))
        self.label_noti.grid(sticky="w", columnspan=3, row=8, column=0, padx=(20,0), pady=(20,10))

        # Button config
        self.btn_save = tk.Button(self.top, font=("Calibri", 12), text="Save", width=8, bg='#0a75ad', fg='#ffffff', command=lambda: self.add_new_connection())
        self.btn_save.grid(sticky="e", row=8, column=3, padx=(0,15), pady=(20,10))

    def close(self):
        self.top.destroy()

    def option_changed(self, *args):
        if self.engine.get() == 'ORACLE':
            self.label_service_name.grid(sticky="w", row=3, column=0, padx=(20, 0), pady=2)
            self.text_service_name.grid(columnspan=3, row=3, column=1, padx=(0, 15), pady=2)
            self.label_service_name_value.set("Service name")
        elif self.engine.get() == 'DB2':
            self.label_service_name.grid(sticky="w", row=3, column=0, padx=(20, 0), pady=2)
            self.text_service_name.grid(columnspan=3, row=3, column=1, padx=(0, 15), pady=2)
            self.label_service_name_value.set("Database")
        else:
            self.label_service_name.grid_forget()
            self.text_service_name.grid_forget()

    def clear_input(self):
        self.text_src_name.delete(0, 'end')
        self.engine.set("Select Engine")
        self.text_host.delete(0, 'end')
        self.text_port.delete(0, 'end')
        self.text_username.delete(0, 'end')
        self.text_pass.delete(0, 'end')
        self.text_schema.delete(0, 'end')
        self.text_username.delete(0, 'end')

    def add_new_connection(self):
        # Get data from input console
        input_src_name = self.text_src_name.get().strip()
        input_engine = self.engine.get()
        input_host = self.text_host.get()
        input_port = self.text_port.get()
        input_username = self.text_username.get()
        input_pass = self.text_pass.get()
        input_schema = self.text_schema.get()

        # Validate input from user
        if input_src_name == "" or input_engine == "Select Engine" or input_host == "" or input_port == "" or input_username == "" or input_pass == "":
            self.label_noti['text'] = 'Vui lòng nhập đầy đủ các thông tin !'
        else:
            #Check SrcName đã tồn tại hay chưa?
            list_cnn_config = config_reader.get_list_src_name_original()
            if input_src_name in list_cnn_config:
                self.label_noti['text'] = 'Tên kết nối đã tồn tại !'
            else:
                if(input_engine == 'ORACLE' or input_engine == 'DB2'):
                    input_service_name = self.text_service_name.get()
                else:
                    input_service_name = ''
                pass_encrypt = encryptor.encrypt(input_pass.encode(),encryptor.key.encode()).decode("utf-8")

                # Make new data:
                new_data = {
                    "schema": input_schema,
                    "server": input_host,
                    "port": input_port,
                    "user": input_username,
                    "password": pass_encrypt,
                    "engine": input_engine,
                    "service_name": input_service_name,
                    "DB": input_service_name
                }

                file = open(IOUtils.get_absolute_path(DefaultVar.DEV_ENV), 'r+')
                config = json.load(file)
                config[input_src_name] = new_data
                file.seek(0)
                json.dump(config, file, indent=4)
                self.label_noti['text'] = 'Thêm mới thành công !!!'
                self.clear_input()


