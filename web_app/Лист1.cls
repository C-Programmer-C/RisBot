VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Лист1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    If Target.CountLarge > 1 Then Exit Sub
    
    ' -------- Сценарий 1: клик в столбце B (продукт) -> FillRange (суммы по месяцам) --------
    If Target.Column = 2 And Target.Row >= 4 Then
        Application.EnableEvents = False
        Dim productName As String
        productName = Trim(Target.Value)
        If productName <> "" Then
            Dim yearCell As Range
            Set yearCell = FindYearCell(Target)
            If Not yearCell Is Nothing Then
                Dim year As Integer
                year = CInt(yearCell.Value)
                Dim ids As String
                ids = GetProductID(productName)
                If ids <> "" Then
                    Dim rng As Range
                    Set rng = Range(Cells(Target.Row, 3), Cells(Target.Row, 14))
                    Call FillRange(year, ids, rng)
                End If
            End If
        End If
        Application.EnableEvents = True
        Exit Sub
    End If
    
    ' -------- Сценарий 2: клик в столбцах C:N (ячейки с данными по месяцам) -> таблица деталей --------
    If Target.Column >= 3 And Target.Column <= 14 And Target.Row >= 4 Then
        Application.EnableEvents = False
        
        ' Получаем продукт из столбца B той же строки
        Dim prodCell As Range
        Set prodCell = Cells(Target.Row, 2)
        Dim prodName As String
        prodName = Trim(prodCell.Value)
        If prodName = "" Then GoTo ExitHandler2
        
        ' Проверяем, есть ли продукт в словаре
        Dim testID As String
        testID = GetProductID(prodName)
        If testID = "" Then GoTo ExitHandler2   ' если ID нет – тихий выход
        
        ' Год из столбца B выше
        Dim yrCell As Range
        Set yrCell = FindYearCell(prodCell)
        If yrCell Is Nothing Then GoTo ExitHandler2
        Dim yr As Integer
        yr = CInt(yrCell.Value)
        
        ' Месяц из текущего столбца выше
        Dim monCell As Range
        Set monCell = FindMonthCell(Target)
        If monCell Is Nothing Then GoTo ExitHandler2
        Dim monthNum As Integer
        monthNum = MonthNumberFromName(Trim(monCell.Value))
        If monthNum = 0 Then GoTo ExitHandler2
        
        ' Счётчик таблиц (A1) определяет, какую таблицу обновлять
        Dim tableNum As Integer
        If IsNumeric(Range("A1").Value) Then
            tableNum = CInt(Range("A1").Value)
            If tableNum < 1 Or tableNum > 2 Then tableNum = 1
        Else
            tableNum = 1
        End If
        
        ' Определяем начальную строку для текущей таблицы
        Dim startRow As Long
        If tableNum = 1 Then
            startRow = 3   ' Заголовок таблицы 1 будет в строке 3
        Else
            ' Для таблицы 2 берём значение из A2 (должно быть число)
            If IsNumeric(Range("A2").Value) Then
                startRow = CLng(Range("A2").Value)
            Else
                startRow = 5   ' Если A2 пусто, по умолчанию строка 5 (под таблицей 1)
            End If
        End If
        
        ' Очищаем область только для этой таблицы
        ClearTable ActiveSheet, startRow, 17   ' 17 - столбец Q
        
        ' Строим таблицу
        Dim dataFound As Boolean
        dataFound = ShowTasksTable(prodName, yr, monthNum, ActiveSheet, startRow, 17)
        
        If dataFound Then
            ' Определяем последнюю строку таблицы
            ' Заголовок таблицы в startRow, заголовки столбцов в startRow+1, данные до последней строки
            ' Нужно найти последнюю непустую ячейку в первом столбце таблицы (или вычислить по данным)
            Dim lastRow As Long
            lastRow = startRow + 1 ' по умолчанию хотя бы заголовки
            If Not IsEmpty(ActiveSheet.Cells(startRow + 1, 17).Value) Then
                ' Ищем последнюю заполненную строку в столбце Q (начиная со строки заголовков)
                lastRow = ActiveSheet.Cells(ActiveSheet.Rows.Count, 17).End(xlUp).Row
                ' Убедимся, что последняя строка не выше заголовка
                If lastRow < startRow Then lastRow = startRow
            End If
            ' Если таблица 1, сохраняем в A2 следующую свободную строку (после таблицы + пустая строка)
            If tableNum = 1 Then
                ' После последней строки данных оставляем одну пустую строку
                Range("A2").Value = lastRow + 2   ' +2 чтобы оставить пустую строку между таблицами
            End If
        Else
            ' Если данных нет, ничего не делаем (или выводим сообщение)
            If Not dataFound Then
                MsgBox "Нет данных за " & monCell.Value & " " & yr & " по продукту " & prodName, vbInformation
            End If
        End If
        
        ' Переключаем счётчик для следующего раза
        Range("A1").Value = IIf(tableNum = 1, 2, 1)
        
ExitHandler2:
        Application.EnableEvents = True
        Exit Sub
    End If
    
    ' Если клик в другом месте – ничего не делаем
End Sub
