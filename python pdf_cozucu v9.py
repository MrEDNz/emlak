#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import sqlite3
import pandas as pd
import pdfplumber
import webbrowser
import traceback
from datetime import datetime
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QPushButton,
    QFileDialog, QLabel, QTableView, QHBoxLayout, QLineEdit,
    QComboBox, QGroupBox, QHeaderView, QSplitter, QListWidget,
    QProgressBar, QMenu, QMessageBox, QInputDialog, QTextEdit,
    QDialog, QTabWidget, QStyle
)
from PySide6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QThread,
    Signal, QObject, QMutex, QMutexLocker, QSize, QTimer
)
from PySide6.QtGui import (
    QCursor, QFontDatabase, QFont, QColor, QGuiApplication
)

# Database configuration
DB_NAME = "real_estate_analysis.db"
STYLESHEET = """
    QMainWindow, QWidget {
        background-color: #2E2E2E;
        color: #F0F0F0;
        font-family: 'Segoe UI';
    }
    QGroupBox {
        font-weight: bold;
        border: 1px solid #555;
        border-radius: 5px;
        margin-top: 1ex;
        font-size: 14px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        left: 10px;
        padding: 0 3px;
    }
    QTableView {
        background-color: #3C3C3C;
        border: 1px solid #555;
        gridline-color: #555;
        selection-background-color: #5A5A5A;
    }
    QHeaderView::section {
        background-color: #4A4A4A;
        padding: 4px;
        border: 1px solid #555;
        font-weight: bold;
    }
    QPushButton {
        background-color: #5A5A5A;
        border: 1px solid #777;
        padding: 8px;
        border-radius: 4px;
        min-width: 80px;
    }
    QPushButton:hover {
        background-color: #6A6A6A;
    }
    QPushButton:pressed {
        background-color: #454545;
    }
    QLineEdit, QComboBox {
        padding: 5px;
        border: 1px solid #555;
        border-radius: 4px;
        background-color: #3C3C3C;
    }
    QComboBox::drop-down {
        border: none;
    }
    QLabel#drop_zone {
        border: 2px dashed #555;
        border-radius: 5px;
        padding: 20px;
        font-size: 16px;
        text-align: center;
        background-color: #333;
    }
    QLabel#drop_zone_active {
        border: 2px solid #00AEEF;
        background-color: #404040;
    }
    QListWidget {
        background-color: #3C3C3C;
        border: 1px solid #555;
    }
    QProgressBar {
        border: 1px solid #555;
        border-radius: 3px;
        text-align: center;
    }
    QProgressBar::chunk {
        background-color: #00AEEF;
    }
    QTextEdit {
        background-color: #3C3C3C;
        color: #F0F0F0;
        font-family: 'Consolas';
    }
    QTabWidget::pane {
        border: 1px solid #555;
    }
    QTabBar::tab {
        background: #4A4A4A;
        color: #F0F0F0;
        padding: 8px;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }
    QTabBar::tab:selected {
        background: #5A5A5A;
        border-bottom: 2px solid #00AEEF;
    }
"""

class DatabaseManager(QObject):
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            super().__init__()
            self.conn = None
            self.mutex = QMutex()
            self._initialized = True
            self.setup_database()

    def setup_database(self):
        """Database bağlantısını ana thread'de oluştur"""
        try:
            with QMutexLocker(self.mutex):
                self.conn = sqlite3.connect(
                    DB_NAME,
                    timeout=10,
                    check_same_thread=False
                )
                self.conn.execute("PRAGMA journal_mode=WAL")
                self.conn.execute("PRAGMA synchronous=NORMAL")
                self.create_tables()
        except Exception as e:
            print(f"Database connection error: {str(e)}")
            raise

    def create_tables(self):
        with QMutexLocker(self.mutex):
            cursor = self.conn.cursor()
            
            tables = {
                "listings": """
                CREATE TABLE IF NOT EXISTS listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ilan_no TEXT UNIQUE,
                    ilce TEXT,
                    semt TEXT,
                    tam_adres TEXT,
                    oda_sayisi TEXT,
                    brut_metrekare REAL,
                    fiyat REAL,
                    ilan_tarihi TEXT,
                    kaynak_dosya TEXT,
                    eklenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                "analysis_history": """
                CREATE TABLE IF NOT EXISTS analysis_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    analysis_type TEXT,
                    parameters TEXT,
                    result_summary TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                "price_history": """
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ilan_no TEXT,
                    fiyat REAL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ilan_no) REFERENCES listings (ilan_no)
                )"""
            }

            for table_name, table_sql in tables.items():
                try:
                    cursor.execute(table_sql)
                except Exception as e:
                    print(f"Error creating table {table_name}: {str(e)}")
            
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_listings_ilan_no ON listings(ilan_no)",
                "CREATE INDEX IF NOT EXISTS idx_listings_ilce ON listings(ilce)",
                "CREATE INDEX IF NOT EXISTS idx_listings_semt ON listings(semt)",
                "CREATE INDEX IF NOT EXISTS idx_price_history_ilan_no ON price_history(ilan_no)"
            ]

            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except Exception as e:
                    print(f"Error creating index: {str(e)}")

            self.conn.commit()

    def save_listings(self, listings):
        try:
            with QMutexLocker(self.mutex):
                cursor = self.conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                
                for listing in listings:
                    cursor.execute(
                        "SELECT fiyat FROM listings WHERE ilan_no = ?",
                        (listing['İlan Numarası'],))
                    existing_price = cursor.fetchone()
                    
                    cursor.execute("""
                    INSERT OR REPLACE INTO listings 
                    (ilan_no, ilce, semt, tam_adres, oda_sayisi, 
                     brut_metrekare, fiyat, ilan_tarihi, kaynak_dosya, 
                     guncellenme_tarihi)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        listing['İlan Numarası'],
                        listing['İlçe'],
                        listing['Semt'],
                        listing['Tam Adres'],
                        listing['Oda Sayısı'],
                        listing['m² (Brüt)'],
                        listing['Fiyat'],
                        listing['İlan Tarihi'],
                        listing['Kaynak Dosya']
                    ))
                    
                    if existing_price and existing_price[0] != listing['Fiyat']:
                        cursor.execute("""
                        INSERT INTO price_history (ilan_no, fiyat)
                        VALUES (?, ?)
                        """, (listing['İlan Numarası'], listing['Fiyat']))
                
                self.conn.commit()
                return True
                
        except sqlite3.Error as e:
            print(f"Database error in save_listings: {str(e)}")
            self.conn.rollback()
            return False
        except Exception as e:
            print(f"Unexpected error in save_listings: {str(e)}")
            self.conn.rollback()
            return False

    def get_all_listings(self, limit=5000):
        with QMutexLocker(self.mutex):
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                SELECT 
                    ilan_no as 'İlan Numarası',
                    ilce as 'İlçe',
                    semt as 'Semt',
                    tam_adres as 'Tam Adres',
                    oda_sayisi as 'Oda Sayısı',
                    brut_metrekare as 'm² (Brüt)',
                    fiyat as 'Fiyat',
                    ilan_tarihi as 'İlan Tarihi',
                    kaynak_dosya as 'Kaynak Dosya'
                FROM listings
                ORDER BY guncellenme_tarihi DESC
                LIMIT ?
                """, (limit,))
                
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
                
            except Exception as e:
                print(f"Error in get_all_listings: {str(e)}")
                return pd.DataFrame()

    def get_existing_listings(self):
        with QMutexLocker(self.mutex):
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT ilan_no FROM listings")
                return {row[0] for row in cursor.fetchall()}
            except Exception as e:
                print(f"Error in get_existing_listings: {str(e)}")
                return set()

    def save_analysis(self, analysis_type, params, result):
        with QMutexLocker(self.mutex):
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                INSERT INTO analysis_history 
                (analysis_type, parameters, result_summary)
                VALUES (?, ?, ?)
                """, (analysis_type, str(params), result[:1000]))
                self.conn.commit()
                return True
            except Exception as e:
                print(f"Error saving analysis: {str(e)}")
                return False

    def get_price_history(self, ilan_no):
        with QMutexLocker(self.mutex):
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                SELECT fiyat, recorded_at 
                FROM price_history 
                WHERE ilan_no = ?
                ORDER BY recorded_at
                """, (ilan_no,))
                return cursor.fetchall()
            except Exception as e:
                print(f"Error getting price history: {str(e)}")
                return []

    def clear_database(self):
        with QMutexLocker(self.mutex):
            try:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM listings")
                cursor.execute("DELETE FROM analysis_history")
                cursor.execute("DELETE FROM price_history")
                self.conn.commit()
                return True
            except Exception as e:
                print(f"Error clearing database: {str(e)}")
                return False

    def backup_database(self, backup_path):
        import shutil
        with QMutexLocker(self.mutex):
            try:
                self.conn.close()
                shutil.copyfile(DB_NAME, backup_path)
                self.setup_database()
                return True
            except Exception as e:
                print(f"Error backing up database: {str(e)}")
                self.setup_database()
                return False

    def close(self):
        with QMutexLocker(self.mutex):
            if self.conn:
                try:
                    self.conn.close()
                except Exception as e:
                    print(f"Error closing database: {str(e)}")

    def __del__(self):
        self.close()

class PandasModel(QAbstractTableModel):
    def __init__(self, data=pd.DataFrame()):
        super().__init__()
        self._data = data
        self._format_cache = {}

    def rowCount(self, parent=QModelIndex()):
        return self._data.shape[0]

    def columnCount(self, parent=QModelIndex()):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
            
        row = index.row()
        col = index.column()
        col_name = self._data.columns[col]
        value = self._data.iloc[row, col]
        
        if role == Qt.DisplayRole:
            cache_key = (row, col)
            if cache_key in self._format_cache:
                return self._format_cache[cache_key]
                
            if col_name == 'Fiyat' and pd.notna(value):
                try:
                    formatted = f"{float(value):,.2f} ₺".replace(",", "X").replace(".", ",").replace("X", ".")
                    self._format_cache[cache_key] = formatted
                    return formatted
                except:
                    return str(value)
            elif col_name == 'm² (Brüt)' and pd.notna(value):
                try:
                    formatted = f"{float(value):,.0f}"
                    self._format_cache[cache_key] = formatted
                    return formatted
                except:
                    return str(value)
            return str(value)
            
        elif role == Qt.ToolTipRole:
            if col_name in ['İlçe', 'Semt', 'Tam Adres']:
                return "Düzenlemek için çift tıklayın\nKopyalamak için sağ tıklayın"
            elif col_name in ['Oda Sayısı', 'm² (Brüt)', 'Fiyat', 'İlan Tarihi']:
                return "Kopyalamak için sağ tıklayın"
            return None
            
        elif role == Qt.TextAlignmentRole:
            if col_name in ['Fiyat', 'm² (Brüt)']:
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter
            
        elif role == Qt.BackgroundRole:
            if col_name == 'Fiyat' and pd.notna(value):
                try:
                    price = float(value)
                    if price > 1000000:
                        return QColor('#3A5A78')
                except:
                    pass
            return None
            
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section] + 1)
        return None

    def update_data(self, data):
        self.beginResetModel()
        self._data = data
        self._format_cache.clear()
        self.endResetModel()

    def flags(self, index):
        default_flags = super().flags(index)
        col_name = self._data.columns[index.column()]
        
        if col_name in ['İlçe', 'Semt', 'Tam Adres']:
            return default_flags | Qt.ItemIsEditable
        return default_flags

    def setData(self, index, value, role=Qt.EditRole):
        if role == Qt.EditRole:
            col_name = self._data.columns[index.column()]
            row = index.row()
            
            if col_name in ['İlçe', 'Semt', 'Tam Adres']:
                self._data.iloc[row, index.column()] = value
                self._format_cache.clear()
                self.dataChanged.emit(index, index)
                
                if col_name in ['İlçe', 'Semt']:
                    self.update_tam_adres(row)
                return True
        return False
    
    def update_tam_adres(self, row):
        ilce = self._data.at[row, 'İlçe']
        semt = self._data.at[row, 'Semt']
        
        if pd.isna(ilce):
            new_adres = None
        elif pd.isna(semt):
            new_adres = ilce
        else:
            new_adres = f"{ilce}/{semt}"
        
        if self._data.at[row, 'Tam Adres'] != new_adres:
            self._data.at[row, 'Tam Adres'] = new_adres
            adres_index = self._data.columns.get_loc('Tam Adres')
            self.dataChanged.emit(
                self.index(row, adres_index),
                self.index(row, adres_index)
            )

class PDFAnalyzerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # 1. Temel ayarlar
        self.setWindowTitle("Emlak Analiz Aracı v2.0")
        self.setMinimumSize(1000, 700)
        self.resize(1400, 900)
        
        # 2. Database bağlantısı
        self.db_manager = DatabaseManager()
        
        # 3. Merkez widget
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        
        # 4. Splitter
        self.splitter = QSplitter(Qt.Horizontal)
        self.splitter.setHandleWidth(5)
        self.splitter.setChildrenCollapsible(False)
        
        # 5. Paneller
        self.left_panel = self.create_left_panel()
        self.right_panel = self.create_right_panel()
        
        # 6. Splitter'a panelleri ekle
        self.splitter.addWidget(self.left_panel)
        self.splitter.addWidget(self.right_panel)
        self.splitter.setSizes([350, 1050])
        
        # 7. Ana layout'a splitter'ı ekle
        self.main_layout.addWidget(self.splitter)
        
        # 8. Model ve View
        self.current_model = PandasModel()
        self.table_view = QTableView()
        self.table_view.setModel(self.current_model)
        
        # 9. Stylesheet
        self.setStyleSheet(STYLESHEET)
        
        # 10. Bağlantıları kur
        self.setup_connections()
        
        # 11. Verileri yükle
        QTimer.singleShot(100, self.load_initial_data)

    def create_left_panel(self):
        panel = QWidget()
        panel.setMinimumWidth(300)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(10)
        
        # PDF List Section
        pdf_group = QGroupBox("PDF Dosyaları")
        pdf_layout = QVBoxLayout(pdf_group)
        
        self.pdf_list_widget = QListWidget()
        self.pdf_list_widget.setMaximumHeight(120)
        self.pdf_list_widget.setSelectionMode(QListWidget.ExtendedSelection)
        
        self.drop_zone_label = QLabel("\nPDF Dosyalarını Buraya Sürükleyin\nveya Butona Tıklayın\n")
        self.drop_zone_label.setAlignment(Qt.AlignCenter)
        self.drop_zone_label.setObjectName("drop_zone")
        
        select_file_button = QPushButton("PDF Dosyaları Seç (Çoklu Seçim)")
        select_file_button.setIcon(self.style().standardIcon(QStyle.SP_DirOpenIcon))
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        
        pdf_layout.addWidget(self.pdf_list_widget)
        pdf_layout.addWidget(self.drop_zone_label)
        pdf_layout.addWidget(select_file_button)
        pdf_layout.addWidget(self.progress_bar)
        
        filter_groupbox = QGroupBox("Gelişmiş Filtreleme")
        filter_layout = QVBoxLayout(filter_groupbox)
        
        self.ilce_combo = QComboBox()
        self.ilce_combo.setEditable(True)
        self.ilce_combo.addItem("Tüm İlçeler")
        
        self.semt_combo = QComboBox()
        self.semt_combo.setEditable(True)
        self.semt_combo.addItem("Tüm Semtler")
        
        self.oda_combo = QComboBox()
        self.oda_combo.addItem("Tüm Oda Sayıları")
        
        filter_layout.addWidget(QLabel("İlçe:"))
        filter_layout.addWidget(self.ilce_combo)
        filter_layout.addWidget(QLabel("Semt:"))
        filter_layout.addWidget(self.semt_combo)
        filter_layout.addWidget(QLabel("Oda Sayısı:"))
        filter_layout.addWidget(self.oda_combo)
        
        layout.addWidget(pdf_group)
        layout.addWidget(filter_groupbox)
        layout.addStretch()
        
        return panel

    def create_right_panel(self):
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.table_view = QTableView()
        self.table_view.setSortingEnabled(True)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setAlternatingRowColors(True)
        
        layout.addWidget(self.table_view)
        return panel

    def setup_connections(self):
        select_file_button = self.findChild(QPushButton, "PDF Dosyaları Seç (Çoklu Seçim)")
        if select_file_button:
            select_file_button.clicked.connect(self.select_pdf_files)

    def load_initial_data(self):
        try:
            df = self.db_manager.get_all_listings(limit=1000)
            if not df.empty:
                self.current_model.update_data(df)
        except Exception as e:
            QMessageBox.critical(self, "Hata", f"Veri yükleme hatası: {str(e)}")

    def select_pdf_files(self):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "PDF Dosyaları Seç", "", "PDF Files (*.pdf)")
        if file_paths:
            self.pdf_list_widget.addItems([os.path.basename(f) for f in file_paths])

def main():
    app = QApplication(sys.argv)
    window = PDFAnalyzerApp()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()