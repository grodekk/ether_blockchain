import os
from PyQt5.QtWidgets import QRadioButton, QApplication, QMainWindow, QAction, QVBoxLayout, QWidget, QPushButton, QHBoxLayout, QSplitter, QSizePolicy, QLabel, QSplitterHandle, QProgressDialog, QProgressBar
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from charts import ChartHandler, chart_config
from functools import partial
import mplcursors
from PyQt5.QtGui import QCursor, QPixmap
from PyQt5.Qt import Qt
from PyQt5.QtCore import QTimer, QObject, QPropertyAnimation, QEasingCurve, QRect, pyqtProperty, pyqtSlot, QMutex, QMutexLocker
from PyQt5.QtWidgets import QDateEdit, QInputDialog, QCalendarWidget, QDialog, QMessageBox, QLineEdit
from PyQt5.QtCore import QDate, pyqtSignal, QThread, QMetaObject 
from database_tool import check_date_in_database
import blocks_download
import blocks_extractor
import wallets_update
from wallets_update import save_top_wallets_info
import database_tool
from datetime import datetime
import blocks_remover
import time

DATABASE_PATH = os.path.join(os.path.dirname(__file__), "baza_danych.db")
BLOCKS_DATA_PATH = os.path.join(os.path.dirname(__file__), "blocks_data")
MINIMUM_WINDOW_WIDTH = 1200
MINIMUM_WINDOW_HEIGHT = 800
MENU_WIDTH_TARGET = 300
CURSOR_MONITOR_INTERVAL = 250
EXPAND_AREA_WIDTH = 15

class BackgroundTask(QObject):
    progress_updated = pyqtSignal(int)
    task_interrupted = pyqtSignal() 

    def __init__(self, task_function, *args, **kwargs):
        super().__init__()
        self.task_function = task_function
        self.args = args
        self.kwargs = kwargs
        self.is_interrupted = False
        

    def run(self):       
                
        try:
            result = self.task_function(*self.args, **self.kwargs, check_interrupt=self.check_interrupt)
            if not self.is_interrupted:
                return result

        except Exception as e:
            print(f"Error: {e}")
            if self.is_interrupted:
                print("BackgroundTask: emitting task_interrupted")
                self.task_interrupted.emit() 

        finally:
            if self.is_interrupted:
                print("BackgroundTask: emitting task_interrupted")
                self.task_interrupted.emit()
        

    def check_interrupt(self):
        return self.is_interrupted

    def interrupt(self):
        if not self.is_interrupted:
            print("interrupt: emitting task_interrupted")
            self.is_interrupted = True
            self.task_interrupted.emit()
    

class WorkerThread(QThread):
    task_finished = pyqtSignal(int)
    progress_updated = pyqtSignal(int)
    task_interrupted = pyqtSignal() 
    
    def __init__(self, background_task):
        super().__init__()
        self.background_task = background_task   
   
    def run(self):
        result = self.background_task.run()
        if not self.background_task.is_interrupted:
            self.task_finished.emit(result)
        else:
            print("Workerthread: emitting task_interrupted")
            self.task_interrupted.emit()
                    
    @pyqtSlot(int)
    def update_progress(self, progress):        
            if not self.is_interrupted:
                self.progress_updated.emit(value)
    

class NoHandleSplitter(QSplitter):
    def createHandle(self):
        return NoHandleSplitterHandle(self.orientation(), self)


class NoHandleSplitterHandle(QSplitterHandle):
    def __init__(self, orientation, parent=None):
        super().__init__(orientation, parent)
        self.setCursor(self.parent().cursor())

    def mousePressEvent(self, event):
        pass  # Blokuj obsługę zdarzenia kliknięcia myszą

    def mouseMoveEvent(self, event):
        pass  # Blokuj obsługę zdarzenia przesunięcia myszą


class EthereumDataApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.pending_task = None
        self.target_date = "2024-01-01"

        self.menu_width = 0    
        self.default_mode_selected = True
        self.manual_mode_selected = False      
        self.auto_mode_selected = False     
        self.progress_dialog = None

        self.background_task = None
        self.worker_thread = None

        self.radio_buttons_disabled = False
        self.auto_mode_running = False
        
        self.error_message_mutex = QMutex()
        self.error_message_shown = False
        self.dialog_closed_by_user = True

        self.setWindowTitle("Analiza danych Ethereum")
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.chart_config = chart_config      
        self.setMinimumSize(MINIMUM_WINDOW_WIDTH, MINIMUM_WINDOW_HEIGHT)
        self.menu_label = QLabel(self)
        self.main_layout = QHBoxLayout()
        self.setup_ui()
        self.current_chart = None

        self.selected_date_hourly = None
        self.is_hourly_chart = False
           
        self.canvas.setVisible(False)
    

    def setup_ui(self):
        self.mouse_monitor_setup()
        self.setup_animation()
        self.create_main_menu()        
        self.create_chart_layout() 
    
        self.create_buttons()
        self.create_logo_label()
        self.create_chart_widget()
        self.create_splitter()
        self.setup_layout()        


    def mouse_monitor_setup(self):
        self.mouse_monitor_timer = QTimer(self)
        self.mouse_monitor_timer.timeout.connect(self.monitor_cursor)
        self.mouse_monitor_timer.start(CURSOR_MONITOR_INTERVAL)


    def setup_animation(self):       
        self.animation = QPropertyAnimation(self)
        self.animation.setTargetObject(self)
        self.animation.setPropertyName(b"menuWidth")
        self.animation.setDuration(150)
        self.animation.setEasingCurve(QEasingCurve.InOutQuad)        


    def create_main_menu(self):       
        menubar = self.menuBar()
        file_menu = menubar.addMenu("Menu")
        
        settings_action = QAction("Ustawienia", self)
        settings_action.triggered.connect(self.show_settings_dialog)
        save_action = QAction("?", self)
        exit_action = QAction("Wyjście", self)
        exit_action.triggered.connect(self.close)        
       
        file_menu.addAction(settings_action)        
        file_menu.addAction(save_action)
        file_menu.addSeparator() 
        file_menu.addAction(exit_action)


    def create_chart_handler(self):
        self.chart_handler = ChartHandler(self.canvas, DATABASE_PATH)


    def create_buttons(self):
        buttons_layout = QVBoxLayout()

        for category_name, category_data in self.chart_config.items():
            
            category_label = QLabel(category_name)
            category_label.setStyleSheet("font-weight: bold;")
            buttons_layout.addWidget(category_label)
           
            for chart_name, chart_data in category_data.items():
                button = QPushButton(chart_name)
                button.setMinimumWidth(10)
                button.clicked.connect(partial(self.show_chart, chart_name, category_name))
                buttons_layout.addWidget(button)

        buttons_layout.addStretch(1)
        buttons_widget = QWidget()
        buttons_widget.setLayout(buttons_layout)
        self.buttons_widget = buttons_widget


    def create_chart_layout(self):
        self.chart_layout = QVBoxLayout()
        self.canvas = FigureCanvas(Figure(figsize=(10, 6)))
        self.chart_layout.addWidget(self.canvas)


    def create_logo_label(self):
        self.logo_label = QLabel(self)
        pixmap = QPixmap("E:\projekty\ethereum_blockchain_analysis\ethereum_logo.png")
        scaled_pixmap = pixmap.scaled(500, 500, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.logo_label.setPixmap(scaled_pixmap)
        self.logo_label.setAlignment(Qt.AlignCenter)
        self.chart_layout.addWidget(self.logo_label)

    def create_chart_widget(self):
        self.chart_widget = QWidget()
        self.chart_widget.setLayout(self.chart_layout)


    def create_splitter(self):
        self.splitter = NoHandleSplitter(Qt.Horizontal)          
        self.splitter.addWidget(self.buttons_widget)
        self.splitter.addWidget(self.chart_widget)
        self.splitter.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)        
        self.splitter.setSizes([self.menu_width, self.width() - self.menu_width])  
            

    def setup_layout(self):
        self.main_layout.addWidget(self.splitter)
        self.central_widget.setLayout(self.main_layout)


    def get_single_date_from_user(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Wybierz datę")
        dialog.setGeometry(800, 300, 300, 300)

        calendar = QCalendarWidget(dialog)
        calendar.setGeometry(10, 10, 280, 200)
        
        ok_button = QPushButton("OK", dialog)
        ok_button.setGeometry(10, 220, 100, 30)
        cancel_button = QPushButton("Anuluj", dialog)
        cancel_button.setGeometry(190, 220, 100, 30)

        selected_date = None

        def on_ok_click():
            nonlocal selected_date
            selected_date = calendar.selectedDate()
            dialog.accept()

        ok_button.clicked.connect(on_ok_click)
        cancel_button.clicked.connect(dialog.reject)

        result = dialog.exec_()

        return selected_date.toPyDate() if result == QDialog.Accepted else None


    def show_chart(self, chart_name, category_name):
        self.logo_label.setVisible(False)
        self.canvas.setVisible(True)
      
        chart_data = self.chart_config.get(category_name).get(chart_name)

        if chart_data:
            sql_query_base = chart_data.get("sql_query")                
            if "hourly" in sql_query_base.lower():
                self.is_hourly_chart = True
                selected_date = self.get_single_date_from_user()
                if not selected_date:
                    self.is_hourly_chart = False 
                    self.show_error_message("Data nie istnieje w bazie danych dla wykresu {}.".format(chart_name))
                    return
                self.selected_date_hourly = selected_date
                                
                sql_query = chart_data.get("sql_query")                               
                sql_query_check = sql_query + ' AND DATE(date) = DATE(?)'
                          
                if not check_date_in_database(selected_date, chart_name, sql_query_check, DATABASE_PATH):
                    self.show_error_message("Data nie istnieje w bazie danych dla wykresu {}.".format(chart_name))
                    return
            else:
                self.is_hourly_chart = False 

            self.current_chart_name = chart_name
            label = chart_data.get("label")
            title = chart_data.get("title")
            ylabel = chart_data.get("ylabel")

            if self.current_chart:
                self.current_chart.close_chart()

            self.current_chart = ChartHandler(self.canvas, DATABASE_PATH)

            if self.is_hourly_chart:
                data_type = chart_data.get("data_type")                  
                formatted_selected_date = self.selected_date_hourly.strftime("%Y-%m-%d")
                sql_query = f'{sql_query} AND DATE(date) = DATE("{formatted_selected_date}")' 
               
            else:              
                sql_query = chart_data.get("sql_query")                
            self.current_chart.chart_builder(sql_query, label, title, ylabel)


    def show_error_message(self, message):
        error_dialog = QMessageBox()
        error_dialog.setIcon(QMessageBox.Critical)
        error_dialog.setWindowTitle("Błąd")
        error_dialog.setText(message)
        error_dialog.exec_()


    def getMenuWidth(self):
            return self.menu_width


    def setMenuWidth(self, width):        
            self.menu_width = width
            self.splitter.setSizes([self.menu_width, self.splitter.width() - self.menu_width])

    menuWidth = pyqtProperty(int, getMenuWidth, setMenuWidth)


    def monitor_cursor(self):
        for widget in QApplication.topLevelWidgets():
            if widget.isVisible() and widget != self:
                return

        cursor_position = QCursor.pos()
        window_position = self.mapFromGlobal(cursor_position)

        expand_area_height = self.height() // 2
        menu_expand_area_x = 0
        menu_expand_area_y = self.height() // 4

        expand_area = QRect(menu_expand_area_x, menu_expand_area_y, EXPAND_AREA_WIDTH, expand_area_height)

        if expand_area.contains(window_position):
            self.menu_hovered = True
        else:
            self.menu_hovered = False
            self.menu_label.setAlignment(Qt.AlignCenter)
            self.menu_label.setStyleSheet(
                "color: black;"
                "background-color: lightblue;"
                "border: 2px solid black;"
            )
            self.menu_label.setText("W\nY\nK\nR\nE\nS\nY")
            self.menu_label.setFixedWidth(EXPAND_AREA_WIDTH)
            self.menu_label.setGeometry(menu_expand_area_x, menu_expand_area_y, EXPAND_AREA_WIDTH, expand_area_height)
            self.menu_label.show()

        if self.menu_hovered:
            self.animation.setStartValue(self.menu_width)
            self.animation.setEndValue(MENU_WIDTH_TARGET)
            self.animation.start()
        elif not self.menu_hovered and self.menu_width > 0 and window_position.x() > MENU_WIDTH_TARGET:
            self.animation.setStartValue(self.menu_width)
            self.animation.setEndValue(0)
            self.animation.start()


    def show_settings_dialog(self):
                                        
                self.settings_dialog = QDialog(self)
                self.settings_dialog.setWindowTitle("Ustawienia")
                self.settings_dialog.setGeometry(700, 300, 400, 300) 

                self.default_mode_radio = QRadioButton("brak trybu", self.settings_dialog)
                self.auto_mode_radio = QRadioButton("Tryb automatyczny", self.settings_dialog)
                self.manual_mode_radio = QRadioButton("Tryb manualny", self.settings_dialog)

                self.default_mode_radio.setChecked(self.default_mode_selected)
                self.manual_mode_radio.setChecked(self.manual_mode_selected)
                self.auto_mode_radio.setChecked(self.auto_mode_selected)

                if self.radio_buttons_disabled:
                    self.default_mode_radio.setEnabled(False)
                    self.manual_mode_radio.setEnabled(False)

                if not self.radio_buttons_disabled:
                    self.default_mode_radio.setEnabled(True)
                    self.manual_mode_radio.setEnabled(True)

                additional_options_label = QLabel("Dodatkowe opcje (dla trybu manualnego):", self.settings_dialog)
                additional_auto_options_label = QLabel("Dodatkowe opcje (dla trybu automatycznego):", self.settings_dialog)
                additional_options_label.setEnabled(False)
                additional_auto_options_label.setEnabled(False)

                target_date_label = QLabel("Target Date (YYYY-MM-DD):", self.settings_dialog)
                self.target_date_input = QDateEdit(QDate.fromString(self.target_date, "yyyy-MM-dd"), self.settings_dialog)
                self.target_date_input.setCalendarPopup(True)
                self.target_date_input.setDisplayFormat("yyyy-MM-dd")
                self.target_date_input.setDate(self.target_date_input.date()) 
                self.target_date_input.setDisabled(True)                 
                
                self.progress_bar = QProgressBar(self.settings_dialog)
                self.progress_bar.setGeometry(30, 220, 340, 25)
                self.progress_bar.setMaximum(100)
                self.progress_bar.setValue(0)
               
                self.auto_cancel_button = QPushButton("PRZERWIJ")
                if not self.auto_mode_running:
                    self.auto_cancel_button.setEnabled(False) 
                self.auto_cancel_button.clicked.connect(self.cancel_task)                                

                settings_layout = QVBoxLayout()
                settings_layout.addWidget(self.default_mode_radio)

                settings_layout.addWidget(self.auto_mode_radio)
                settings_layout.addWidget(additional_auto_options_label)  
                settings_layout.addWidget(target_date_label)  
                settings_layout.addWidget(self.target_date_input)
                settings_layout.addWidget(self.progress_bar) 
                settings_layout.addWidget(self.auto_cancel_button)

                settings_layout.addWidget(self.manual_mode_radio)
                settings_layout.addWidget(additional_options_label)           
           
                fetch_blocks_count_button = QPushButton("Pobierz ostatnie bloki", self.settings_dialog)     
                fetch_blocks_count_button.clicked.connect(self.get_blocks_count)
                fetch_blocks_count_button.setEnabled(self.manual_mode_selected) 
                settings_layout.addWidget(fetch_blocks_count_button)

               
                extract_daily_data_button = QPushButton("Wykonaj ekstrakcję danych dziennych", self.settings_dialog)                
                extract_daily_data_button.clicked.connect(lambda: self.show_extract_data_dialog(blocks_extractor.extract_daily_data))
                extract_daily_data_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(extract_daily_data_button)

                
                extract_hourly_data_button = QPushButton("Wykonaj ekstrakcję danych godzinowych", self.settings_dialog)                
                extract_hourly_data_button.clicked.connect(lambda: self.show_extract_data_dialog(blocks_extractor.extract_hourly_data))
                extract_hourly_data_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(extract_hourly_data_button)

                
                update_top_wallets_button = QPushButton("Zaaktualizuj salda TOP portfeli", self.settings_dialog)                
                update_top_wallets_button.clicked.connect(self.update_top_wallets)
                update_top_wallets_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_top_wallets_button)

                
                update_database_button = QPushButton("Eksportuj dane do bazy danych", self.settings_dialog)                
                update_database_button.clicked.connect(self.update_database)
                update_database_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_database_button)

                
                update_top_wallets_database = QPushButton("Eksportuj dane TOP portfeli do bazy danych", self.settings_dialog)                
                update_top_wallets_database.clicked.connect(self.update_top_wallets_database)
                update_top_wallets_database.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_top_wallets_database)

                
                remove_blocks_button = QPushButton("Usuń bloki w przedziale czasowym", self.settings_dialog)
                remove_blocks_button.clicked.connect(self.remove_blocks_dialog)
                remove_blocks_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(remove_blocks_button)

                settings_layout.addStretch(1)

                self.ok_button = QPushButton("OK", self.settings_dialog)
                cancel_button = QPushButton("Anuluj", self.settings_dialog)

                settings_layout.addWidget(self.ok_button)
                settings_layout.addWidget(cancel_button)

                self.settings_dialog.setLayout(settings_layout)

                
                self.auto_mode_radio.toggled.connect(lambda: self.target_date_input.setEnabled(self.auto_mode_radio.isChecked()))
                self.auto_mode_radio.toggled.connect(lambda: additional_auto_options_label.setEnabled(self.auto_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: additional_options_label.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: extract_daily_data_button.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: extract_hourly_data_button.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: update_top_wallets_button.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: update_database_button.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: update_top_wallets_database.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: remove_blocks_button.setEnabled(self.manual_mode_radio.isChecked()))
                self.manual_mode_radio.toggled.connect(lambda: fetch_blocks_count_button.setEnabled(self.manual_mode_radio.isChecked()))

         
                self.ok_button.clicked.connect(self.settings_dialog.accept)
                if self.auto_mode_running:
                    self.ok_button.setEnabled(False)

                cancel_button.clicked.connect(self.settings_dialog.reject)

                result = self.settings_dialog.exec_()       
               
                if result == QDialog.Accepted:
                    self.default_mode_selected = self.default_mode_radio.isChecked()
                    self.auto_mode_selected = self.auto_mode_radio.isChecked()
                    self.manual_mode_selected = self.manual_mode_radio.isChecked()
                    self.target_date = self.target_date_input.text()                    

                    if self.auto_mode_selected:
                        self.run_auto_mode(self.target_date)


    def disable_radio_buttons(self):
        self.radio_buttons_disabled = True
        print('buttons disabled!')


    def enable_radio_buttons(self):
        self.radio_buttons_disabled = False


    def handle_task_finished(self):        
        self.dialog_closed_by_user = False       
        print("Handling task finished")
        if self.progress_dialog:
            print("Closing progress dialog.")
            self.progress_dialog.close()
            self.progress_dialog = None

            
        if self.pending_task:
                print('pending task executing')
                self.pending_task()
                self.pending_task = None
        
        else:
            QMessageBox.information(self, "Zadanie zakończone", "Zadanie zostało zakończone pomyślnie.")
        self.dialog_closed_by_user = True
       
        self.background_task = None
        self.worker_thread = None
        

    @pyqtSlot(int)
    def update_progress(self, progress):           
        if self.settings_dialog and self.settings_dialog.isVisible():
            if self.auto_mode_selected and self.progress_bar:
                print(f"Updating QProgressBar with value: {progress}")
                self.progress_bar.setValue(progress)
        
            elif self.progress_dialog and self.progress_dialog.isVisible():
                self.progress_dialog.setValue(progress)
        

    @pyqtSlot(int, int, bool)     
    def update_progress_callback(self, total, progress_value):            
        if self.auto_mode_selected:          
            progress_percentage = (progress_value / total) * 100            
            progress_percentage = int(progress_percentage)  

            if self.background_task:                          
                self.background_task.progress_updated.emit(progress_percentage)

        if self.progress_dialog:
            progress_percentage = (progress_value / total) * 100
            progress_percentage = int(progress_percentage)    
            if self.background_task:
                self.background_task.progress_updated.emit(progress_percentage) 

        if self.worker_thread.background_task.is_interrupted:
            print("update_progress_callback: emitting task_interrupted")
            self.worker_thread.background_task.task_interrupted.emit()
                    

    def execute_task(self, task_function, *args, use_dialog=True, progress_callback=None):
        if use_dialog and not self.progress_dialog:
            self.progress_dialog = QProgressDialog("Pobieranie danych...", "Przerwij", 0, 100, self)
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.canceled.connect(self.handle_dialog_canceled)

            self.progress_dialog.show()
            print("Progress dialog created and connected to cancel_task.")

        if self.background_task:
            print('JEST BACKGROUND TASK')

        self.background_task = BackgroundTask(task_function, *args, progress_callback=self.update_progress_callback)
        self.background_task.progress_updated.connect(self.update_progress)
        self.background_task.task_interrupted.connect(self.on_task_interrupted)


        if self.worker_thread:
            print('JEST WORKER TRHEAD!')

        self.worker_thread = WorkerThread(self.background_task)
        self.worker_thread.task_finished.connect(self.handle_task_finished)     
        self.worker_thread.task_interrupted.connect(self.on_task_interrupted) 
        self.worker_thread.start()


    def handle_dialog_canceled(self):
        print("handling dialog cancel")
        if self.dialog_closed_by_user:
            self.cancel_task()

    def cancel_task(self):
        print("cancel_task called.")
        print("Progress Dialog Canceled")
        print("cancel_task: emitting task_interrupted")
        self.auto_mode_running = False
        self.auto_cancel_button.setEnabled(False)
        if self.background_task:
             self.background_task.interrupt()


    def on_task_interrupted(self):
            print("on_task_interrupted called")
            print(f"Caller: {self.sender()}")
            if self.background_task:
                print(self.background_task.check_interrupt())
            
            if self.progress_dialog:
                self.progress_dialog.close()
                self.progress_dialog = None
            
            print(self.error_message_shown)
            if not self.error_message_shown:
                self.error_message_shown = True
                self.worker_thread.quit()
                self.worker_thread.wait()            
                self.show_error_message("Zadanie zostało przerwane.")
                self.error_message_shown = False
            
            self.background_task = None
            self.worker_thread = None
            self.auto_mode_ended()

                
    def auto_mode_ended(self):
        self.auto_mode_running = False
        self.ok_button.setEnabled(True)
        self.enable_radio_buttons()
        self.default_mode_radio.setEnabled(True)
        self.manual_mode_radio.setEnabled(True)
        self.progress_bar.setValue(0)


    def run_auto_mode(self, target_date):        
        update_interval = 0.1
        print("Running auto mode")     
        self.auto_mode_running = True
        self.auto_cancel_button.setEnabled(True)
        self.ok_button.setEnabled(False)
        self.disable_radio_buttons() 
        self.execute_task(self.execute_automator_task, target_date, update_interval, use_dialog=False, progress_callback=self.update_progress_callback)
        

    def execute_automator_task(self, target_date, update_interval, progress_callback=None, check_interrupt=None):
        import automation        
        automator = automation.BlockAutomator(target_date, update_interval, progress_callback=progress_callback, check_interrupt=check_interrupt)                
        automator.run()


    def get_blocks_count(self):
        num_blocks, ok_pressed = blocks_download.BlockInput.get_num_blocks_to_fetch(method="interface")
        main_app = blocks_download.MainBlockProcessor(blocks_download.Config())

        if ok_pressed:
            print(f"Liczba bloków do pobrania: {num_blocks}")            
            self.execute_task(main_app.run, num_blocks)
        else:
            print("Anulowano pobieranie liczby bloków")


    def show_extract_data_dialog(self, extraction_function):
        selected_date = self.get_single_date_from_user()

        if selected_date:
            formatted_date = selected_date.strftime("%Y-%m-%d %H:%M:%S")           
            self.execute_task(extraction_function, formatted_date)

  
    def update_top_wallets(self):
        selected_date = self.get_single_date_from_user()

        if selected_date:
            formatted_date = selected_date.strftime("%Y-%m-%d")
            input_file_name = f"{formatted_date}_daily_data.json"            
            self.execute_task(wallets_update.save_top_wallets_info, input_file_name)


    def update_top_wallets_database(self):

        input_file_name = "Biggest_wallets_activity.json"
        database_tool.save_biggest_wallets_activity_database(input_file_name, DATABASE_PATH)   


    def update_database(self):
        selected_date = self.get_single_date_from_user()

        if selected_date:            
            
            formatted_date = selected_date.strftime("%Y-%m-%d")
            input_file_name = f"{formatted_date}_daily_data.json"
            data_type = "daily"            

            def run_hourly_data_task():
                input_file_name = f"{formatted_date}_hourly_data.json"
                data_type = "hourly"            
                self.execute_task(database_tool.import_data_to_combined_table, input_file_name, DATABASE_PATH, data_type)
            
            self.pending_task = run_hourly_data_task
            self.execute_task(database_tool.import_data_to_combined_table, input_file_name, DATABASE_PATH, data_type)


    def remove_blocks_dialog(self):
        start_date = self.get_single_date_from_user()
        if start_date is None:
            return

        end_date = self.get_single_date_from_user()
        if end_date is None:
            return

        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        self.execute_task(blocks_remover.remove_blocks_in_time_range, BLOCKS_DATA_PATH, start_datetime, end_datetime)


if __name__ == "__main__":
    app = QApplication([])
    main_window = EthereumDataApp()
    main_window.show()
    app.exec_()
