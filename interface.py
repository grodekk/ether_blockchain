import os
from PyQt5.QtWidgets import QRadioButton, QApplication, QMainWindow, QAction, QVBoxLayout, QWidget, QPushButton, QHBoxLayout, QSplitter, QSizePolicy, QLabel, QSplitterHandle, QProgressDialog
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from charts import ChartHandler, chart_config
from functools import partial
import mplcursors
from PyQt5.QtGui import QCursor, QPixmap
from PyQt5.Qt import Qt
from PyQt5.QtCore import QTimer, QObject, QPropertyAnimation, QEasingCurve, QRect, pyqtProperty, pyqtSlot
from PyQt5.QtWidgets import QDateEdit, QInputDialog, QCalendarWidget, QDialog, QMessageBox
from PyQt5.QtCore import QDate, pyqtSignal, QThread, QMetaObject 
from database_tool import check_date_in_database
import blocks_download
import blocks_extractor
import wallets_update
from wallets_update import save_top_wallets_info
import database_tool
from datetime import datetime
import blocks_remover



# Stałe
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "baza_danych.db")
BLOCKS_DATA_PATH = os.path.join(os.path.dirname(__file__), "blocks_data")
MINIMUM_WINDOW_WIDTH = 1200
MINIMUM_WINDOW_HEIGHT = 800
MENU_WIDTH_TARGET = 300
CURSOR_MONITOR_INTERVAL = 250
EXPAND_AREA_WIDTH = 15

class BackgroundTask(QObject):
    progress_updated = pyqtSignal(int)

    def __init__(self, task_function, *args, **kwargs):
        super().__init__()
        self.task_function = task_function
        self.args = args
        self.kwargs = kwargs

    def run(self):                
        result = self.task_function(*self.args, **self.kwargs)   
        return result


class WorkerThread(QThread):
    task_finished = pyqtSignal(int)
    progress_updated = pyqtSignal(int)

    def __init__(self, background_task):
        super().__init__()
        self.background_task = background_task   
   
    def run(self):
        result = self.background_task.run()      
        self.task_finished.emit(result)

    @pyqtSlot(int)
    def update_progress(self, progress):        
        self.progress_updated.emit(progress)


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
        self.pending_task = None  # Tu przechowujemy funkcję, która zostanie uruchomiona po zakończeniu aktualnego zadania

        self.menu_width = 0    
        self.manual_mode_selected = False      

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
            # Dodaj nagłówek kategorii
            category_label = QLabel(category_name)
            category_label.setStyleSheet("font-weight: bold;")
            buttons_layout.addWidget(category_label)

            # Przejdź przez wykresy w danej kategorii
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
                                        
                settings_dialog = QDialog(self)
                settings_dialog.setWindowTitle("Ustawienia")
                settings_dialog.setGeometry(700, 300, 400, 300)

                auto_mode_radio = QRadioButton("Tryb automatyczny", settings_dialog)
                manual_mode_radio = QRadioButton("Tryb manualny", settings_dialog)
                auto_mode_radio.setChecked(not self.manual_mode_selected)  
                manual_mode_radio.setChecked(self.manual_mode_selected)

                additional_options_label = QLabel("Dodatkowe opcje (dla trybu manualnego):", settings_dialog)
                additional_options_label.setEnabled(False)

                settings_layout = QVBoxLayout()
                settings_layout.addWidget(auto_mode_radio)
                settings_layout.addWidget(manual_mode_radio)
                settings_layout.addWidget(additional_options_label)           
           
                fetch_blocks_count_button = QPushButton("Pobierz ostatnie bloki", settings_dialog)     
                fetch_blocks_count_button.clicked.connect(self.get_blocks_count)
                fetch_blocks_count_button.setEnabled(self.manual_mode_selected) 
                settings_layout.addWidget(fetch_blocks_count_button)

                # Dodaj przycisk do ekstrakcji danych dziennych
                extract_daily_data_button = QPushButton("Wykonaj ekstrakcję danych dziennych", settings_dialog)                
                extract_daily_data_button.clicked.connect(lambda: self.show_extract_data_dialog(blocks_extractor.extract_daily_data))
                extract_daily_data_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(extract_daily_data_button)

                # Dodaj przycisk do ekstrakcji danych godzinowych
                extract_hourly_data_button = QPushButton("Wykonaj ekstrakcję danych godzinowych", settings_dialog)                
                extract_hourly_data_button.clicked.connect(lambda: self.show_extract_data_dialog(blocks_extractor.extract_hourly_data))
                extract_hourly_data_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(extract_hourly_data_button)

                # Dodaj przycisk do aktualizacji top portfeli
                update_top_wallets_button = QPushButton("Zaaktualizuj salda TOP portfeli", settings_dialog)                
                update_top_wallets_button.clicked.connect(self.update_top_wallets)
                update_top_wallets_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_top_wallets_button)

                # Dodaj przycisk do aktualizacji bazy danych
                update_database_button = QPushButton("Eksportuj dane do bazy danych", settings_dialog)                
                update_database_button.clicked.connect(self.update_database)
                update_database_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_database_button)

                # Dodaj przycisk do aktualizacji bazy danych TOP portfeli
                update_top_wallets_database = QPushButton("Eksportuj dane TOP portfeli do bazy danych", settings_dialog)                
                update_top_wallets_database.clicked.connect(self.update_top_wallets_database)
                update_top_wallets_database.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(update_top_wallets_database)

                    # Dodaj przycisk do usuwania bloków w przedziale czasowym
                remove_blocks_button = QPushButton("Usuń bloki w przedziale czasowym", settings_dialog)
                remove_blocks_button.clicked.connect(self.remove_blocks_dialog)
                remove_blocks_button.setEnabled(self.manual_mode_selected)
                settings_layout.addWidget(remove_blocks_button)


                settings_layout.addStretch(1)

                ok_button = QPushButton("OK", settings_dialog)
                cancel_button = QPushButton("Anuluj", settings_dialog)

                settings_layout.addWidget(ok_button)
                settings_layout.addWidget(cancel_button)

                settings_dialog.setLayout(settings_layout)

                # Połącz zdarzenia
                manual_mode_radio.toggled.connect(lambda: additional_options_label.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: extract_daily_data_button.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: extract_hourly_data_button.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: update_top_wallets_button.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: update_database_button.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: update_top_wallets_database.setEnabled(manual_mode_radio.isChecked()))
                manual_mode_radio.toggled.connect(lambda: remove_blocks_button.setEnabled(manual_mode_radio.isChecked()))

                 # Dodaj sygnał monitorujący zmiany w zaznaczeniu radia automatyczny/manualny
                auto_mode_radio.toggled.connect(lambda: fetch_blocks_count_button.setEnabled(not auto_mode_radio.isChecked()))
         
                ok_button.clicked.connect(settings_dialog.accept)
                cancel_button.clicked.connect(settings_dialog.reject)

                # Tutaj ustaw przycisk jako nieaktywny, gdy tryb jest automatyczny
                fetch_blocks_count_button.setEnabled(not auto_mode_radio.isChecked())

                result = settings_dialog.exec_()             
               
                if result == QDialog.Accepted:
                    if auto_mode_radio.isChecked():
                        print("Tryb automatyczny")
                    elif manual_mode_radio.isChecked():
                        print("Tryb manualny")
                        # Nie uruchamiaj pobierania bloków, tylko ustaw flagę trybu manualnego
                        self.manual_mode_selected = True


    def handle_task_finished(self):
        # Zamykamy okno dialogowe po zakończeniu zadania
        print("Handling task finished")
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None

            # Jeśli jest jeszcze jakieś zadanie w kolejce, to je uruchamiamy
        if self.pending_task:
                self.pending_task()
                self.pending_task = None

        # Dodaj komunikat o zakończeniu zadania
        else:
            QMessageBox.information(self, "Zadanie zakończone", "Zadanie zostało zakończone pomyślnie.")

    @pyqtSlot(int)
    def update_progress(self, progress):       
        # Aktualizujemy pasek postępu w oknie dialogowym
        if self.progress_dialog:
            self.progress_dialog.setValue(progress)


    @pyqtSlot(int, int)     
    def update_progress_callback(self, total, progress_value):
        # Aktualizuj pasek postępu w oknie dialogowym
        if self.progress_dialog:
            progress_percentage = (progress_value / total) * 100
            progress_percentage = int(progress_percentage)    
            if self.background_task:
                self.background_task.progress_updated.emit(progress_percentage)
                    

    def execute_task(self, task_function, *args, progress_callback=None):

        self.progress_dialog = QProgressDialog("Pobieranie danych...", None, 0, 100, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()

        self.background_task = BackgroundTask(task_function, *args, progress_callback=self.update_progress_callback)
        self.background_task.progress_updated.connect(self.update_progress)
        self.worker_thread = WorkerThread(self.background_task)
        self.worker_thread.task_finished.connect(self.handle_task_finished)
        self.worker_thread.start()


    def get_blocks_count(self):
        num_blocks, ok_pressed = blocks_download.get_num_blocks_to_fetch("interface")

        if ok_pressed:
            print(f"Liczba bloków do pobrania: {num_blocks}")            
            self.execute_task(blocks_download.main, num_blocks)
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

            # Ustawiamy funkcję jako "oczekujące zadanie", które zostanie uruchomione po zakończeniu pierwszego zadania
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
