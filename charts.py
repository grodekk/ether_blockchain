import sqlite3
import mplcursors
from config import Config
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from error_handler import ErrorHandler

chart_config = {
    "WYKRESY GODZINOWE": {
        "ILOŚĆ TRANSAKCJI": {
            "sql_query": 'SELECT date, transactions_number FROM combined_data WHERE data_type="hourly"',
            "label": "Liczba transakcji",
            "title": "Liczba transakcji w poszczególnych godzinach ",
            "ylabel": "Liczba transakcji",
        },
        "ŚREDNIE OPŁATY TRANSAKCYJNE": {
            "sql_query": 'SELECT date, average_transaction_fee FROM combined_data WHERE data_type="hourly"',
            "label": "Średnia opłata transakcyjna",
            "title": "Średnia opłata transakcyjna w poszczególnych godzinach",
            "ylabel": "Opłata",
        },
        " PORTFELE PONIŻEJ 0.1 ETH": {
            "sql_query": 'SELECT date, wallet_0_1_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 0.1- ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        " PORTFELE 0.1-1 ETH": {
            "sql_query": 'SELECT date, wallet_0_1_to_1_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 0.1-1 ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 1-10 ETH": {
            "sql_query": 'SELECT date, wallet_1_10_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 1-10 ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 10-100 ETH": {
            "sql_query": 'SELECT date, wallet_10_100_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 10-100 ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 100-1000 ETH": {
            "sql_query": 'SELECT date, wallet_100_1000_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 100-1000 ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 1000-10000 ETH": {
            "sql_query": 'SELECT date, wallet_1000_10000_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 1000-10000 ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE  POWYŻEJ 10000 ETH": {
            "sql_query": 'SELECT date, wallet_above_10000_eth FROM combined_data WHERE data_type="hourly"',
            "label": "Portfele 10000+ ETH", 
            "title": "Salda w poszczególnych godzinach",
            "ylabel": "Ilość portfeli",
        }

    },
    "WYKRESY DZIENNE":{
        "ILOŚĆ TRANSAKCJI": {
            "sql_query": 'SELECT date, transactions_number FROM combined_data WHERE data_type="daily"',
            "label": "Liczba transakcji",
            "title": "Liczba transakcji w poszczególnych dniach",
            "ylabel": "Liczba transakcji",
        },
        "ŚREDNIE OPŁATY TRANSAKCYJNE": {
            "sql_query": 'SELECT date, average_transaction_fee FROM combined_data WHERE data_type="daily"',
            "label": "Średnia opłata transakcyjna",
            "title": "Średnia opłata transakcyjna w poszczególnych dniach",
            "ylabel": "Opłata",
            },
        "PORTFELE <0.1 ETH": {
            "sql_query": 'SELECT date, wallet_0_1_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele poniżej 0.1 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
    },
        "PORTFELE 0.1-1 ETH d": {
            "sql_query": 'SELECT date, wallet_0_1_to_1_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 0-1 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
    },
        "PORTFELE 1-10 ETH": {
            "sql_query": 'SELECT date, wallet_1_10_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 1-10 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 10-100 ETH": {
            "sql_query": 'SELECT date, wallet_10_100_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 10-100 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 100-1000 ETH": {
            "sql_query": 'SELECT date, wallet_100_1000_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 100-1000 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE 1000-10000 ETH": {
            "sql_query": 'SELECT date, wallet_1000_10000_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 1000-10000 ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
        },
        "PORTFELE  POWYŻEJ 10000 ETH": {
            "sql_query": 'SELECT date, wallet_above_10000_eth FROM combined_data WHERE data_type="daily"',
            "label": "Portfele 10000+ ETH", 
            "title": "Salda w poszczególnych dniach",
            "ylabel": "Ilość portfeli",
        }
        
    },
      "WYKRESY NAJWIĘKSZYCH PORTFELI":{
        "TOP 1 PORTFEL": {
            "sql_query" : '''
                            SELECT wb.date, wb.balance
                            FROM wallet_balance AS wb
                            WHERE wb.wallet_address = (
                                SELECT wallet_address 
                                FROM wallet_balance 
                                GROUP BY wallet_address
                                ORDER BY SUM(balance) DESC
                                LIMIT 1
                            )
                            ORDER BY wb.date
                        ''',
            "label": "Saldo ETH ",
            "title": "Saldo ETH TOP 1 portfela",
            "ylabel": "Saldo w ETH",
        },
        "TOP 2 PORTFEL": {            
            "sql_query": '''SELECT wb.date, wb.balance
                            FROM wallet_balance AS wb
                            WHERE wb.wallet_address = (
                                SELECT wallet_address 
                                FROM (
                                    SELECT wallet_address 
                                    FROM wallet_balance 
                                    GROUP BY wallet_address
                                    ORDER BY SUM(balance) DESC 
                                    LIMIT 1 OFFSET 1
                                ) AS top_wallet
                            )
                            ORDER BY wb.date''',
            "label": "Saldo ETH",
            "title": "Saldo w ETH TOP 2 portfela",
            "ylabel": "Saldo w ETH",
        },
        "TOP 3 PORTFEL": {            
            "sql_query": '''SELECT wb.date, wb.balance
                            FROM wallet_balance AS wb
                            WHERE wb.wallet_address = (
                                SELECT wallet_address 
                                FROM (
                                    SELECT wallet_address 
                                    FROM wallet_balance 
                                    GROUP BY wallet_address
                                    ORDER BY SUM(balance) DESC 
                                    LIMIT 1 OFFSET 2
                                ) AS top_wallet
                            )
                            ORDER BY wb.date''',
            "label": "Saldo ETH",
            "title": "Saldo ETH TOP 3 portfela",
            "ylabel": "Saldo w ETH",
        },
        "TOP 4 PORTFEL": {            
            "sql_query": '''SELECT wb.date, wb.balance
                            FROM wallet_balance AS wb
                            WHERE wb.wallet_address = (
                                SELECT wallet_address 
                                FROM (
                                    SELECT wallet_address 
                                    FROM wallet_balance 
                                    GROUP BY wallet_address
                                    ORDER BY SUM(balance) DESC 
                                    LIMIT 1 OFFSET 3
                                ) AS top_wallet
                            )
                            ORDER BY wb.date''',
            "label": "Saldo ETH",
            "title": "Saldo ETH TOP 4 portfela",
            "ylabel": "Saldo w ETH",
        },
        "TOP 5 PORTFEL": {            
            "sql_query": '''SELECT wb.date, wb.balance
                        FROM wallet_balance AS wb
                        WHERE wb.wallet_address = (
                            SELECT wallet_address 
                            FROM (
                                SELECT wallet_address 
                                FROM wallet_balance 
                                GROUP BY wallet_address
                                ORDER BY SUM(balance) DESC 
                                LIMIT 1 OFFSET 4
                            ) AS top_wallet
                        )
                        ORDER BY wb.date''',
            "label": "Saldo ETH",
            "title": "Saldo ETH TOP 5 portfela",
            "ylabel": "Saldo w ETH",
        },
 
}
}

@ErrorHandler.ehdc()
class ChartHandler:
    def __init__(self, canvas, db_filename):
        self.canvas = canvas
        self.db_filename = db_filename
        self.current_annotation = None
        self.selected_index = None

    def close_chart(self):        
        self.canvas.figure.clf()
        self.canvas.draw()
           

    def chart_builder(self, sql_query, label, title, ylabel):      
        conn = sqlite3.connect(self.db_filename)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()      

        data_x = [row[0] for row in data]                
        data_y = [row[1] for row in data]
        
         
        sorted_indices = sorted(range(len(data_x)), key=lambda i: data_x[i])
        data_x = [data_x[i] for i in sorted_indices]
        data_y = [data_y[i] for i in sorted_indices]

        fig = self.canvas.figure
        ax = fig.add_subplot(111)
        line, = ax.plot(data_x, data_y, marker='o', color='b', label=label)
        ax.fill_between(data_x, data_y, color='lightblue', alpha=0.3)
        ax.set_title(title)
        ax.set_xlabel('Data')
        ax.set_ylabel(ylabel)
        ax.grid(True, linestyle='--', alpha=0.3)

        scatter = ax.scatter(data_x, data_y, color='b')
        data_cursor = mplcursors.cursor(scatter, hover=True)
        

        def on_add(sel):            

            ind = int(sel.index)
            self.selected_index = ind
            x = data_x[ind]
            y = data_y[ind]
            
            annotation = sel.annotation        
                
            self.current_annotation = annotation
            
            annotation.set_text(f'Data: {x}\n\n{label}: {y}')                    

            if len(data_x) >= 2:
                x_shift = 0.125 * len(data_x)
                if ind >= len(data_x) // 2:
                    ha = 'right'
                    x_shift = -x_shift
                else:
                    ha = 'left'
   
            else:
                ha = 'left'
                x_shift = 0.01 * len(data_x)                

            bbox_props = dict(
                boxstyle='square,pad=1',
                facecolor='white',
                edgecolor='gray',
                alpha=1,
                linewidth=2,
                pad=20,                            )

            arrow_props=dict(
                arrowstyle='fancy',
                connectionstyle='arc3,rad=0.5',               
                ec='r',                
                )
                                
            annotation.set_bbox(bbox_props)                        
            annotation.set_fontsize(10)
            annotation.set_fontweight('normal')
            annotation.set_fontfamily('Arial')
            annotation.set_ha(ha)
            x_shift_total = ind + x_shift
            annotation.set_position((x_shift_total, y))
            annotation.set_va('center')
            annotation.set_visible(True)
            line.set_alpha(0.3)
            scatter.set_alpha(0.1)            
            annotation.arrow_patch.set(**arrow_props)
            
           
        def on_move(event):
            if event.inaxes == ax:
                contains, _ = scatter.contains(event)
                if not contains:
                    if self.current_annotation is not None:
                        self.current_annotation.set_visible(False)
                        line.set_alpha(1.0)
                        scatter.set_alpha(1.0)
                        fig.canvas.draw_idle()
                else:
                    self.current_annotation.set_visible(True)
                    fig.canvas.draw_idle()
                    line.set_alpha(0.3)
                    if self.selected_index is not None:
                        alphas = [0.1 if i != self.selected_index else 1.0 for i in range(len(data_y))]
                        scatter.set_alpha(alphas)

        fig.canvas.mpl_connect('motion_notify_event', on_move)
        data_cursor.connect("add", on_add)

        visible_xticks_indices = [0, len(data_x) // 3, 2 * len(data_x) // 3, len(data_x) - 1]
        visible_xticks = [data_x[i] for i in visible_xticks_indices]
        ax.set_xticks(visible_xticks)
        ax.set_xticklabels([data_x[i] for i in visible_xticks_indices])

        fig.autofmt_xdate()
        self.canvas.draw()    
    
        conn.close() 


if __name__ == "__main__":
    """
    mainly for testing
    """

    config = Config()
    db_filename = config.DB_FILENAME

    chart_config = {
        "WYKRESY DZIENNE": {
            "ILOŚĆ TRANSAKCJI": {
                "sql_query": 'SELECT date, transactions_number FROM combined_data WHERE data_type="daily"',
                "label": "Liczba transakcji",
                "title": "Liczba transakcji w poszczególnych dniach",
                "ylabel": "Liczba transakcji",
            }
        }
    }

    try:
        fig = plt.figure(figsize=(10, 6))
        canvas = FigureCanvas(fig)

        chart_data = chart_config["WYKRESY DZIENNE"]["ILOŚĆ TRANSAKCJI"]
        sql_query = chart_data["sql_query"]
        label = chart_data["label"]
        title = chart_data["title"]
        ylabel = chart_data["ylabel"]

        chart_handler = ChartHandler(canvas, db_filename)

        chart_handler.chart_builder(sql_query, label, title, ylabel)

        canvas.draw()
        plt.show()
        print("Wykres został wyświetlony poprawnie.")

    except Exception as e:
        print(f"Error: {e}")