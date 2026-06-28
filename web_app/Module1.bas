Attribute VB_Name = "Module1"
Option Explicit

Private Const AUTH_URL As String = "https://accounts.pyrus.com/api/v4/auth"
Private Const REGISTER_URL As String = "https://api.pyrus.com/v4/forms/1562280/register"
Private Const LOGIN As String = "masha25mary@gmail.com"
Private Const SECURITY_KEY As String = "MRqGWTuY6hVPwm-llkS~xREzPYQnpk9n5oZKi3d6X4xdeYJwgmDfPpA-2OKTSKxp17yZ-xXRqPBqFu3c9RwxQYSKDwLX0rdt"

' Находит ячейку с годом выше по столбцу
Public Function FindYearCell(ByVal StartCell As Range) As Range
    Dim r As Long
    r = StartCell.Row - 1
    While r >= 1
        Dim cell As Range
        Set cell = Cells(r, StartCell.Column)
        Dim val As Variant
        val = cell.Value
        If IsNumeric(val) Then
            Dim y As Long
            y = CLng(val)
            If y >= 1900 And y <= 2100 Then
                Set FindYearCell = cell
                Exit Function
            End If
        End If
        r = r - 1
    Wend
    Set FindYearCell = Nothing
End Function

' ---------- Словарь ID продуктов ----------
' Добавляйте новые продукты сюда по образцу
Public Function GetProductID(productName As String) As String
    Dim name As String
    name = Trim(productName)
    
    Select Case name
        Case "Мука рисовая В.С."
            GetProductID = "165022095"
        Case "Мука рисовая 1 С."
            GetProductID = "165022096"
        Case "Мука рисовая 2 С."
            GetProductID = "165022097"
        Case "Дробь"
            GetProductID = "165022089,165022091,165022092"
        Case "Рис"
            GetProductID = "176538234,165022085,167224299,175075715,165022086"
        Case "Крупа"
            GetProductID = "165022099,170169477,165022100,170169479,165022101,170169480,165022102,170169481,165022103,170169483"
        Case "Мука"
            GetProductID = "165022104,170827124,170827127,170827123,165022105,170827131"
        Case "Кормовые"
            GetProductID = "165022107,165022108,165022109,165022110,165022111,165022112,165022113"
        Case Else
            GetProductID = ""
    End Select
End Function

' ---------- Функции для работы с API (без изменений) ----------
Public Function GetAccessToken() As String
    Dim http As Object
    Set http = CreateObject("WinHttp.WinHttpRequest.5.1")
    
    Dim requestBody As String
    requestBody = "{""login"":""" & LOGIN & """,""security_key"":""" & SECURITY_KEY & """}"
    
    http.Open "POST", AUTH_URL, False
    http.SetRequestHeader "Content-Type", "application/json"
    http.Send requestBody
    
    If http.Status <> 200 Then
        Err.Raise vbObjectError + 1, "GetAccessToken", "Ошибка авторизации: " & http.Status & " - " & http.StatusText
    End If
    
    GetAccessToken = ExtractJsonValue(http.responseText, "access_token")
End Function

Public Function ExtractJsonValue(ByVal jsonText As String, ByVal key As String) As String
    Dim pattern As String
    pattern = """" & key & """:\s*""([^""]+)"""
    Dim regex As Object
    Set regex = CreateObject("VBScript.RegExp")
    regex.Global = False
    regex.IgnoreCase = False
    regex.pattern = pattern
    
    Dim matches As Object
    Set matches = regex.Execute(jsonText)
    If matches.Count > 0 Then
        ExtractJsonValue = matches(0).SubMatches(0)
    Else
        ExtractJsonValue = ""
    End If
End Function

Public Function ExtractTotalKgFromResponse(ByVal responseText As String) As Double
    Dim regex As Object
    Set regex = CreateObject("VBScript.RegExp")
    regex.Global = True
    regex.IgnoreCase = False
    regex.pattern = """id"":4.*?""value"":\s*([0-9.]+)"
    
    Dim matches As Object
    Set matches = regex.Execute(responseText)
    
    Dim total As Double
    total = 0
    Dim match As Object
    For Each match In matches
        total = total + CDbl(match.SubMatches(0))
    Next
    
    ExtractTotalKgFromResponse = total
End Function

Public Function GetTotalForMonth(year As Integer, monthNum As Integer, idsStr As String, token As String) As Double
    Dim http As Object
    Set http = CreateObject("WinHttp.WinHttpRequest.5.1")
    
    Dim firstDay As String, lastDay As String
    firstDay = Format(DateSerial(year, monthNum, 1), "yyyy-mm-dd")
    lastDay = Format(DateSerial(year, monthNum + 1, 1) - 1, "yyyy-mm-dd")
    
    Dim dateCondition As String
    dateCondition = "gt" & firstDay & ",lt" & lastDay
    
    Dim requestBody As String
    requestBody = "{"
    requestBody = requestBody & """fld1"": """ & dateCondition & ""","
    requestBody = requestBody & """include_archived"": ""y"","
    requestBody = requestBody & """fld6"": """ & idsStr & ""","
    requestBody = requestBody & "}"
    
    http.Open "POST", REGISTER_URL, False
    http.SetRequestHeader "Content-Type", "application/json"
    http.SetRequestHeader "Authorization", "Bearer " & token
    http.Send requestBody
    
    If http.Status <> 200 Then
        Err.Raise vbObjectError + 2, "GetTotalForMonth", "Ошибка запроса данных: " & http.Status & " - " & http.StatusText
    End If
    
    GetTotalForMonth = ExtractTotalKgFromResponse(http.responseText)
End Function

Public Function MonthNumberFromName(monthName As String) As Integer
    Dim mn As String
    mn = LCase(Trim(monthName))
    Select Case mn
        Case "январь": MonthNumberFromName = 1
        Case "февраль": MonthNumberFromName = 2
        Case "март": MonthNumberFromName = 3
        Case "апрель": MonthNumberFromName = 4
        Case "май": MonthNumberFromName = 5
        Case "июнь": MonthNumberFromName = 6
        Case "июль": MonthNumberFromName = 7
        Case "август": MonthNumberFromName = 8
        Case "сентябрь": MonthNumberFromName = 9
        Case "октябрь": MonthNumberFromName = 10
        Case "ноябрь": MonthNumberFromName = 11
        Case "декабрь": MonthNumberFromName = 12
        Case Else: MonthNumberFromName = 0
    End Select
End Function

Public Function FindMonthCell(ByVal StartCell As Range) As Range
    Dim r As Long
    r = StartCell.Row - 1
    While r >= 1
        Dim cell As Range
        Set cell = Cells(r, StartCell.Column)
        Dim val As String
        val = Trim(cell.Value)
        If val <> "" Then
            Dim mn As Integer
            mn = MonthNumberFromName(val)
            If mn > 0 Then
                Set FindMonthCell = cell
                Exit Function
            End If
        End If
        r = r - 1
    Wend
    Set FindMonthCell = Nothing
End Function

' ---------- Основная функция для заполнения диапазона ----------
Public Sub FillRange(ByVal year As Integer, ByVal ids As String, TargetRange As Range)
    On Error GoTo ErrorHandler
    
    Dim token As String
    token = GetAccessToken()
    
    Dim cell As Range
    For Each cell In TargetRange
        Dim monthCell As Range
        Set monthCell = FindMonthCell(cell)
        If monthCell Is Nothing Then
            MsgBox "Не удалось найти название месяца выше ячейки " & cell.Address, vbExclamation
            cell.Value = CVErr(xlErrValue)
        Else
            Dim monthName As String
            monthName = Trim(monthCell.Value)
            Dim monthNum As Integer
            monthNum = MonthNumberFromName(monthName)
            If monthNum = 0 Then
                MsgBox "Не удалось определить месяц в ячейке " & monthCell.Address, vbExclamation
                cell.Value = CVErr(xlErrValue)
            Else
                Dim total As Double
                total = GetTotalForMonth(year, monthNum, ids, token)
                If total = 0 Then
                    cell.Value = "0"
                Else
                    cell.Value = total
                End If
            End If
        End If
    Next cell
    
    Exit Sub
    
ErrorHandler:
    MsgBox "Ошибка в FillRange: " & Err.Description, vbCritical
End Sub


' Декодирование UTF-8 строки в ANSI (для корректного отображения кириллицы)
Private Function DecodeUTF8(ByVal utf8Str As String) As String
    Dim utf8Bytes() As Byte
    utf8Bytes = StrConv(utf8Str, vbFromUnicode)
    Dim ans As Object
    Set ans = CreateObject("System.Text.UTF8Encoding")
    DecodeUTF8 = ans.GetString(utf8Bytes)
End Function


' ------------------------------------------------------------------
' Новые функции для получения детальных задач и построения таблиц
' ------------------------------------------------------------------

Private Function DecodeUTF8Bytes(bytes() As Byte) As String
    Dim stream As Object
    Set stream = CreateObject("ADODB.Stream")
    stream.Type = 1 ' adTypeBinary
    stream.Open
    stream.Write bytes
    stream.Position = 0
    stream.Type = 2 ' adTypeText
    stream.Charset = "utf-8"
    DecodeUTF8Bytes = stream.ReadText
    stream.Close
    Set stream = Nothing
End Function

' Получение списка задач по году, месяцу, ID продукта
Public Function GetTasksForPeriod(year As Integer, monthNum As Integer, idsStr As String, token As String) As Collection
    Dim http As Object
    Set http = CreateObject("WinHttp.WinHttpRequest.5.1")
    
    Dim firstDay As String, lastDay As String
    firstDay = Format(DateSerial(year, monthNum, 1), "yyyy-mm-dd")
    lastDay = Format(DateSerial(year, monthNum + 1, 1) - 1, "yyyy-mm-dd")
    
    Dim dateCondition As String
    dateCondition = "gt" & firstDay & ",lt" & lastDay
    
    Dim requestBody As String
    requestBody = "{"
    requestBody = requestBody & """fld1"": """ & dateCondition & ""","
    requestBody = requestBody & """include_archived"": ""y"","
    requestBody = requestBody & """fld6"": """ & idsStr & ""","
    requestBody = requestBody & "}"
    
    http.Open "POST", REGISTER_URL, False
    http.SetRequestHeader "Content-Type", "application/json"
    http.SetRequestHeader "Authorization", "Bearer " & token
    http.Send requestBody
    
    If http.Status <> 200 Then
        Err.Raise vbObjectError + 2, "GetTasksForPeriod", "Ошибка запроса данных: " & http.Status & " - " & http.StatusText
    End If
    
    ' ???????? ????? ??? ????? ? ?????????? ?? UTF-8
    Dim responseBody() As Byte
    responseBody = http.responseBody
    Dim responseText As String
    responseText = DecodeUTF8Bytes(responseBody)
    Debug.Print Left(responseText, 500)
    Debug.Print "========================================"
    ' ?????? ??????
    Dim tasksJson As String
    tasksJson = ExtractTasksArray(responseText)
    If tasksJson = "" Then
        Set GetTasksForPeriod = New Collection
        Exit Function
    End If
    Debug.Print "Extracted tasksJson length: " & Len(tasksJson)
    Dim tasks As Collection
    Set tasks = SplitJsonObjects(tasksJson)
    Set GetTasksForPeriod = tasks
End Function

' Извлекает строку с массивом tasks из полного JSON
Public Function ExtractTasksArray(ByVal fullJson As String) As String
    Dim regex As Object
    Set regex = CreateObject("VBScript.RegExp")
    regex.Global = False
    regex.IgnoreCase = False
    regex.pattern = """tasks""\s*:\s*(\[[\s\S]*\])"
    
    Dim matches As Object
    Set matches = regex.Execute(fullJson)
    If matches.Count > 0 Then
        ExtractTasksArray = matches(0).SubMatches(0)
    Else
        ExtractTasksArray = ""
    End If
End Function

' Разбивает строку с массивом объектов на отдельные объекты (упрощённо)
Public Function SplitJsonObjects(ByVal tasksArray As String) As Collection
    Dim col As New Collection
    Dim level As Integer
    Dim inString As Boolean
    Dim objStart As Long
    Dim i As Long
    Dim prevChar As String
    
    tasksArray = Trim(tasksArray)
    
    For i = 1 To Len(tasksArray)
        Dim ch As String
        ch = Mid(tasksArray, i, 1)
        
        ' Обработка строк с учётом экранирования
        If ch = """" Then
            ' Проверяем, не является ли это экранированной кавычкой
            If i > 1 Then
                prevChar = Mid(tasksArray, i - 1, 1)
                If prevChar <> "\" Then
                    inString = Not inString
                End If
            Else
                inString = Not inString
            End If
        End If
        
        If Not inString Then
            If ch = "{" Then
                If level = 0 Then objStart = i
                level = level + 1
            ElseIf ch = "}" Then
                level = level - 1
                If level = 0 And objStart > 0 Then
                    Dim obj As String
                    obj = Mid(tasksArray, objStart, i - objStart + 1)
                    ' Добавляем только объекты, содержащие поле "fields" (это задачи)
                    If InStr(obj, """fields"":") > 0 Then
                        col.Add obj
                    End If
                    objStart = 0
                End If
            End If
        End If
    Next i
    
    Set SplitJsonObjects = col
End Function


Private Function ExtractCatalogValue(taskJson As String, fieldId As Integer) As String
    ' Ищем поле catalog с указанным id
    Dim idPattern As String
    idPattern = """id"":" & fieldId
    Dim idPos As Long
    idPos = InStr(taskJson, idPattern)
    If idPos = 0 Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    ' Ищем "value":{...}
    Dim valuePos As Long
    valuePos = InStr(idPos, taskJson, """value"":{")
    If valuePos = 0 Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    ' Ищем "rows":[["
    Dim rowsPos As Long
    rowsPos = InStr(valuePos, taskJson, """rows"":[[")
    If rowsPos = 0 Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    ' Перемещаемся на начало первой строки
    Dim strStart As Long
    strStart = rowsPos + Len("""rows"":[[")
    If strStart > Len(taskJson) Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    ' Ищем закрывающую кавычку первого элемента
    Dim quotePos As Long
    quotePos = InStr(strStart, taskJson, """")
    If quotePos = 0 Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    ' Ищем следующую кавычку
    Dim endQuotePos As Long
    endQuotePos = InStr(quotePos + 1, taskJson, """")
    If endQuotePos = 0 Then
        ExtractCatalogValue = ""
        Exit Function
    End If
    
    Dim val As String
    val = Mid(taskJson, quotePos + 1, endQuotePos - quotePos - 1)
    val = Replace(val, "\""", """")
    ExtractCatalogValue = val
End Function

' Извлечение простого значения поля по id (строковый парсер)
Private Function ExtractFieldValue(taskJson As String, fieldId As Integer) As String
    ' Ищем позицию "id":fieldId
    Dim idPattern As String
    idPattern = """id"":" & fieldId
    Dim idPos As Long
    idPos = InStr(taskJson, idPattern)
    If idPos = 0 Then
        ExtractFieldValue = ""
        Exit Function
    End If
    
    ' Ищем "value": после этой позиции
    Dim valuePos As Long
    valuePos = InStr(idPos, taskJson, """value"":")
    If valuePos = 0 Then
        ExtractFieldValue = ""
        Exit Function
    End If
    
    ' Перемещаемся за двоеточие и пробелы
    Dim valStart As Long
    valStart = valuePos + Len("""value"":")
    While valStart <= Len(taskJson) And Mid(taskJson, valStart, 1) = " "
        valStart = valStart + 1
    Wend
    
    ' Определяем тип значения
    Dim firstChar As String
    firstChar = Mid(taskJson, valStart, 1)
    
    If firstChar = """" Then
        ' Строка в кавычках – ищем закрывающую кавычку с учётом экранирования
        Dim i As Long
        Dim inString As Boolean
        inString = False
        Dim valEnd As Long
        For i = valStart + 1 To Len(taskJson)
            Dim ch As String
            ch = Mid(taskJson, i, 1)
            If ch = """" Then
                ' Проверяем, не экранирована ли кавычка
                If Mid(taskJson, i - 1, 1) <> "\" Then
                    valEnd = i
                    Exit For
                End If
            End If
        Next i
        If valEnd > valStart Then
            Dim val As String
            val = Mid(taskJson, valStart + 1, valEnd - valStart - 1)
            val = Replace(val, "\""", """")
            ExtractFieldValue = val
        Else
            ExtractFieldValue = ""
        End If
    ElseIf firstChar = "{" Or firstChar = "[" Then
        ' Вложенный объект или массив – для простых полей возвращаем пустую строку
        ExtractFieldValue = ""
    ElseIf IsNumeric(firstChar) Or firstChar = "-" Then
        ' Число (целое или с точкой)
        Dim numStart As Long
        numStart = valStart
        While numStart <= Len(taskJson) And (IsNumeric(Mid(taskJson, numStart, 1)) Or Mid(taskJson, numStart, 1) = "." Or Mid(taskJson, numStart, 1) = "-")
            numStart = numStart + 1
        Wend
        val = Mid(taskJson, valStart, numStart - valStart)
        If val = "null" Then val = ""
        ExtractFieldValue = val
    ElseIf LCase(Mid(taskJson, valStart, 4)) = "null" Then
        ExtractFieldValue = ""
    Else
        ' В противном случае ищем до запятой или закрывающей скобки
        Dim commaPos As Long
        commaPos = InStr(valStart, taskJson, ",")
        Dim bracePos As Long
        bracePos = InStr(valStart, taskJson, "}")
        Dim endPos As Long
        If commaPos > 0 And bracePos > 0 Then
            endPos = WorksheetFunction.Min(commaPos, bracePos)
        ElseIf commaPos > 0 Then
            endPos = commaPos
        ElseIf bracePos > 0 Then
            endPos = bracePos
        Else
            endPos = 0
        End If
        If endPos > 0 Then
            val = Trim(Mid(taskJson, valStart, endPos - valStart))
            If Left(val, 1) = """" Then val = Mid(val, 2, Len(val) - 2)
            ExtractFieldValue = val
        Else
            ExtractFieldValue = ""
        End If
    End If
End Function

' Извлечение значения из вложенного объекта (multiple_choice, form_link и т.д.)
Private Function ExtractNestedValue(taskJson As String, fieldId As Integer, nestedKey As String) As String
    ' Ищем позицию "id":fieldId
    Dim idPos As Long
    idPos = InStr(taskJson, """id"":" & fieldId)
    If idPos = 0 Then
        ExtractNestedValue = ""
        Exit Function
    End If
    
    ' Находим начало объекта value: "value":{
    Dim valueStart As Long
    valueStart = InStr(idPos, taskJson, """value"":{")
    If valueStart = 0 Then
        ExtractNestedValue = ""
        Exit Function
    End If
    
    ' Находим ключ nestedKey внутри этого объекта
    Dim keyPos As Long
    keyPos = InStr(valueStart, taskJson, """" & nestedKey & """")
    If keyPos = 0 Then
        ExtractNestedValue = ""
        Exit Function
    End If
    
    ' Ищем двоеточие после ключа
    Dim colonPos As Long
    colonPos = InStr(keyPos, taskJson, ":")
    If colonPos = 0 Then
        ExtractNestedValue = ""
        Exit Function
    End If
    
    ' Находим начало значения (после пробелов)
    Dim valStart As Long
    valStart = colonPos + 1
    While Mid(taskJson, valStart, 1) = " " And valStart < Len(taskJson)
        valStart = valStart + 1
    Wend
    
    ' Определяем тип значения
    Dim firstChar As String
    firstChar = Mid(taskJson, valStart, 1)
    
    If firstChar = """" Then
        ' Строка в кавычках
        Dim inString As Boolean
        Dim i As Long
        Dim valEnd As Long
        inString = False
        For i = valStart + 1 To Len(taskJson)
            Dim ch As String
            ch = Mid(taskJson, i, 1)
            If ch = """" Then
                ' Проверяем, не экранирована ли кавычка
                If Mid(taskJson, i - 1, 1) <> "\" Then
                    valEnd = i
                    Exit For
                End If
            End If
        Next i
        If valEnd > 0 Then
            Dim val As String
            val = Mid(taskJson, valStart + 1, valEnd - valStart - 1)
            ' Заменяем экранированные кавычки
            val = Replace(val, "\""", """")
            ExtractNestedValue = val
            Exit Function
        End If
    ElseIf firstChar = "[" Then
        ' Массив
        Dim bracketLevel As Integer
        bracketLevel = 1
        valEnd = valStart
        For i = valStart + 1 To Len(taskJson)
            ch = Mid(taskJson, i, 1)
            If ch = "[" Then bracketLevel = bracketLevel + 1
            If ch = "]" Then bracketLevel = bracketLevel - 1
            If bracketLevel = 0 Then
                valEnd = i
                Exit For
            End If
        Next i
        If valEnd > valStart Then
            val = Mid(taskJson, valStart + 1, valEnd - valStart - 1)
            ' Упрощённо берём первый элемент
            Dim parts() As String
            parts = Split(val, ",")
            If UBound(parts) >= 0 Then
                val = Trim(parts(0))
                If Left(val, 1) = """" Then val = Mid(val, 2, Len(val) - 2)
                ExtractNestedValue = val
                Exit Function
            End If
        End If
    Else
        ' Число или null
        Dim numEnd As Long
        numEnd = valStart
        While IsNumeric(Mid(taskJson, numEnd, 1)) Or Mid(taskJson, numEnd, 1) = "." Or Mid(taskJson, numEnd, 1) = "-"
            numEnd = numEnd + 1
        Wend
        val = Mid(taskJson, valStart, numEnd - valStart)
        If val = "null" Then val = ""
        ExtractNestedValue = val
        Exit Function
    End If
    
    ExtractNestedValue = ""
End Function

Public Function ShowTasksTable(productName As String, year As Integer, monthNum As Integer, targetSheet As Worksheet, startRow As Long, startCol As Long) As Boolean
    On Error GoTo ErrorHandler

    Dim idsStr As String
    idsStr = GetProductID(productName)
    If idsStr = "" Then
        ShowTasksTable = False
        Exit Function
    End If

    Dim token As String
    token = GetAccessToken()

    Dim tasks As Collection
    Set tasks = GetTasksForPeriod(year, monthNum, idsStr, token)

    If tasks.Count = 0 Then
        ShowTasksTable = False
        Exit Function
    End If

    ' ------------------------------------------------------
    ' 1. Фильтрация задач (объём != 0 и цена за кг != 0)
    ' ------------------------------------------------------
    Dim filteredTasks As New Collection
    Dim taskItem As Variant
    Dim taskIndex As Long
    taskIndex = 0

    For Each taskItem In tasks
        taskIndex = taskIndex + 1
        Dim taskJson As String
        taskJson = CStr(taskItem)

        ' --- объём (id=35) ---
        Dim volumeStr As String
        volumeStr = ExtractFieldValue(taskJson, 35)
        If volumeStr = "" Then volumeStr = "0"
        volumeStr = Replace(volumeStr, ",", ".")
        Dim volumeNum As Double
        volumeNum = val(volumeStr)

        ' --- цена за кг (id=12) ---
        Dim pricePerKgStr As String
        pricePerKgStr = ExtractFieldValue(taskJson, 12)

        ' Если не удалось извлечь через ExtractFieldValue, пробуем ручной парсинг
        If pricePerKgStr = "" Then
            pricePerKgStr = ManualExtractPricePerKg(taskJson)
        End If

        Dim pricePerKgNum As Double
        If pricePerKgStr <> "" Then
            Dim cleanPrice As String
            cleanPrice = Trim(Replace(pricePerKgStr, ",", "."))
            pricePerKgNum = val(cleanPrice)
            ' Отладка: если строка не пустая, но Val дал 0, выводим предупреждение
            If pricePerKgNum = 0 And pricePerKgStr <> "0" Then
                Debug.Print "ВНИМАНИЕ: цена за кг не преобразована: [" & pricePerKgStr & "] -> 0"
            End If
        Else
            pricePerKgNum = 0
        End If

        ' Отладочный вывод первых 5 задач
        If taskIndex <= 5 Then
            Debug.Print "Задача " & taskIndex & ": volume=" & volumeStr & " (" & volumeNum & "), price=" & pricePerKgStr & " (" & pricePerKgNum & ")"
        End If

        ' Условие: объём > 0 И цена > 0
        If volumeNum > 0 And pricePerKgNum > 0 Then
            filteredTasks.Add taskItem
        Else
            If taskIndex <= 5 Then
                If volumeNum = 0 Then Debug.Print "   -> пропущена (volume=0)"
                If pricePerKgNum = 0 Then Debug.Print "   -> пропущена (price=0)"
            End If
        End If
    Next taskItem

    Debug.Print "=== Задач до фильтрации: " & tasks.Count & ", после фильтрации: " & filteredTasks.Count

    If filteredTasks.Count = 0 Then
        ShowTasksTable = False
        Exit Function
    End If

    ' ------------------------------------------------------
    ' 2. Сортировка отфильтрованных задач по объёму (убывание)
    ' ------------------------------------------------------
    Dim sortedIndices As Variant
    SortTasksByVolume filteredTasks, sortedIndices

    ' ------------------------------------------------------
    ' 3. Очистка области и подготовка заголовков
    ' ------------------------------------------------------
    Dim titleRange As Range
    Set titleRange = targetSheet.Range(targetSheet.Cells(startRow, startCol), targetSheet.Cells(startRow, startCol + 10))
    titleRange.UnMerge

    Dim monthName As String
    monthName = Application.WorksheetFunction.Index(Array("Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"), monthNum)
    Dim tableTitle As String
    tableTitle = productName & " за " & monthName & " " & year

    targetSheet.Cells(startRow, startCol).Value = tableTitle
    targetSheet.Cells(startRow, startCol).Font.Bold = True
    titleRange.Merge
    titleRange.HorizontalAlignment = xlCenter

    Dim headerRow As Long
    headerRow = startRow + 1

    Dim headers As Variant
    headers = Array("Дата отгрузки", "Прайс", "объем кг", "Цена за кг", "Цена за кг (Дост)", "Организация", "Цена", "Оплачено", "Поставщик", "Адрес отгрузки", "Новый лид")

    Dim colOffset As Long
    For colOffset = 0 To UBound(headers)
        targetSheet.Cells(headerRow, startCol + colOffset).Value = headers(colOffset)
    Next colOffset

    ' ------------------------------------------------------
    ' 4. Заполнение данными
    ' ------------------------------------------------------
    Dim rowOffset As Long
    rowOffset = 1
    Dim idx As Long
    For idx = 1 To filteredTasks.Count
        taskJson = CStr(filteredTasks(sortedIndices(idx)))

        Dim dateShip As String: dateShip = ExtractFieldValue(taskJson, 1)
        Dim priceName As String: priceName = ExtractFieldValue(taskJson, 39)
        If priceName = "" Then priceName = ExtractCatalogValue(taskJson, 6)
        Dim volume As String: volume = ExtractFieldValue(taskJson, 35)
        Dim pricePerKg As String: pricePerKg = ExtractFieldValue(taskJson, 12)
        If pricePerKg = "" Then pricePerKg = ManualExtractPricePerKg(taskJson)
        Dim pricePerKgDel As String: pricePerKgDel = ExtractFieldValue(taskJson, 14)
        Dim organization As String: organization = ExtractFieldValue(taskJson, 5)
        Dim totalPrice As String: totalPrice = ExtractFieldValue(taskJson, 7)
        Dim paid As String: paid = ExtractFieldValue(taskJson, 18)
        Dim supplier As String: supplier = ExtractNestedValue(taskJson, 28, "choice_names")
        Dim shipAddress As String: shipAddress = ExtractNestedValue(taskJson, 27, "choice_names")
        Dim newLead As String: newLead = ExtractNestedValue(taskJson, 30, "subject")
        newLead = Trim(Replace(newLead, "\", ""))

        targetSheet.Cells(headerRow + rowOffset, startCol).Value = dateShip
        targetSheet.Cells(headerRow + rowOffset, startCol + 1).Value = priceName
        targetSheet.Cells(headerRow + rowOffset, startCol + 2).Value = volume
        targetSheet.Cells(headerRow + rowOffset, startCol + 3).Value = pricePerKg
        targetSheet.Cells(headerRow + rowOffset, startCol + 4).Value = pricePerKgDel
        targetSheet.Cells(headerRow + rowOffset, startCol + 5).Value = organization
        targetSheet.Cells(headerRow + rowOffset, startCol + 6).Value = totalPrice
        targetSheet.Cells(headerRow + rowOffset, startCol + 7).Value = IIf(paid = "checked", "Да", "Нет")
        targetSheet.Cells(headerRow + rowOffset, startCol + 8).Value = supplier
        targetSheet.Cells(headerRow + rowOffset, startCol + 9).Value = shipAddress
        targetSheet.Cells(headerRow + rowOffset, startCol + 10).Value = newLead

        rowOffset = rowOffset + 1
    Next idx

    ' ------------------------------------------------------
    ' 5. Форматирование
    ' ------------------------------------------------------
    If rowOffset > 1 Then
        Dim dataRange As Range
        On Error Resume Next
        Set dataRange = targetSheet.Range(targetSheet.Cells(headerRow + 1, startCol + 2), _
                                             targetSheet.Cells(headerRow + rowOffset - 1, startCol + 10))
        If Not dataRange Is Nothing Then
            dataRange.NumberFormat = "#,##0.00"
        End If

        Dim tableRange As Range
        Set tableRange = targetSheet.Range(targetSheet.Cells(startRow, startCol), _
                                           targetSheet.Cells(headerRow + rowOffset - 1, startCol + 10))
        With tableRange.Borders
            .LineStyle = xlContinuous
            .Weight = xlThin
            .ColorIndex = xlAutomatic
        End With

        With targetSheet.Range(targetSheet.Cells(headerRow, startCol), targetSheet.Cells(headerRow, startCol + 10))
            .Font.Bold = True
        End With

        On Error GoTo 0
    End If

    ' ------------------------------------------------------
    ' 6. Очистка ниже таблицы
    ' ------------------------------------------------------
    Dim lastDataRow As Long
    lastDataRow = headerRow + rowOffset - 1
    Dim clearBelowRange As Range
    Set clearBelowRange = targetSheet.Range(targetSheet.Cells(lastDataRow + 1, startCol), _
                                             targetSheet.Cells(lastDataRow + 1000, startCol + 10))
    clearBelowRange.Clear

    ShowTasksTable = True
    Exit Function

ErrorHandler:
    MsgBox "Ошибка при построении таблицы: " & Err.Description, vbCritical
    ShowTasksTable = False
End Function

' ------------------------------------------------------
' Вспомогательная функция для ручного извлечения цены за кг (поле 12)
' ------------------------------------------------------
Private Function ManualExtractPricePerKg(taskJson As String) As String
    ' Ищем "id":12
    Dim idPos As Long
    idPos = InStr(taskJson, """id"":12")
    If idPos = 0 Then
        ManualExtractPricePerKg = ""
        Exit Function
    End If

    ' Ищем "value": после этого
    Dim valuePos As Long
    valuePos = InStr(idPos, taskJson, """value"":")
    If valuePos = 0 Then
        ManualExtractPricePerKg = ""
        Exit Function
    End If

    ' Перемещаемся за двоеточие
    Dim valStart As Long
    valStart = valuePos + Len("""value"":")
    While valStart <= Len(taskJson) And Mid(taskJson, valStart, 1) = " "
        valStart = valStart + 1
    Wend

    ' Определяем начало значения
    Dim firstChar As String
    firstChar = Mid(taskJson, valStart, 1)

    If firstChar = """" Then
        ' Строка в кавычках – ищем закрывающую
        Dim i As Long
        For i = valStart + 1 To Len(taskJson)
            Dim ch As String
            ch = Mid(taskJson, i, 1)
            If ch = """" Then
                If Mid(taskJson, i - 1, 1) <> "\" Then
                    ManualExtractPricePerKg = Mid(taskJson, valStart + 1, i - valStart - 1)
                    Exit Function
                End If
            End If
        Next i
    ElseIf IsNumeric(firstChar) Or firstChar = "-" Then
        ' Число
        Dim numEnd As Long
        numEnd = valStart
        While numEnd <= Len(taskJson) And (IsNumeric(Mid(taskJson, numEnd, 1)) Or Mid(taskJson, numEnd, 1) = "." Or Mid(taskJson, numEnd, 1) = "-")
            numEnd = numEnd + 1
        Wend
        ManualExtractPricePerKg = Mid(taskJson, valStart, numEnd - valStart)
        Exit Function
    ElseIf LCase(Mid(taskJson, valStart, 4)) = "null" Then
        ManualExtractPricePerKg = ""
        Exit Function
    End If

    ManualExtractPricePerKg = ""
End Function

Public Sub ClearTable(ws As Worksheet, startRow As Long, startCol As Long)
    Dim clearRange As Range
    Set clearRange = ws.Range(ws.Cells(startRow, startCol), ws.Cells(startRow + 100, startCol + 10))
    
    ' Убираем объединение ячеек (на случай, если оно есть)
    On Error Resume Next
    clearRange.UnMerge
    On Error GoTo 0
    
    ' Полностью очищаем диапазон (содержимое, форматы, границы, примечания)
    clearRange.Clear
End Sub

Private Sub SortTasksByVolume(ByRef tasks As Collection, ByRef sortedIndices As Variant)
    ' Создаём массив объёмов
    Dim volumes() As Double
    Dim i As Long, j As Long
    Dim taskCount As Long
    taskCount = tasks.Count
    ReDim volumes(1 To taskCount)
    ReDim sortedIndices(1 To taskCount)
    
    ' Заполняем массив объёмов (id=35)
    For i = 1 To taskCount
        Dim taskJson As String
        taskJson = CStr(tasks(i))
        Dim volumeStr As String
        volumeStr = ExtractFieldValue(taskJson, 35)
        ' Преобразуем строку в число (заменяем запятую на точку)
        If volumeStr <> "" Then
            volumeStr = Replace(volumeStr, ",", ".")
            If IsNumeric(volumeStr) Then
                volumes(i) = CDbl(volumeStr)
            Else
                volumes(i) = 0
            End If
        Else
            volumes(i) = 0
        End If
        sortedIndices(i) = i
    Next i
    
    ' Сортировка пузырьком по убыванию (по объёму)
    For i = 1 To taskCount - 1
        For j = i + 1 To taskCount
            If volumes(i) < volumes(j) Then
                ' Меняем местами объёмы
                Dim tempVol As Double
                tempVol = volumes(i)
                volumes(i) = volumes(j)
                volumes(j) = tempVol
                ' Меняем местами индексы
                Dim tempIndex As Long
                tempIndex = sortedIndices(i)
                sortedIndices(i) = sortedIndices(j)
                sortedIndices(j) = tempIndex
            End If
        Next j
    Next i
End Sub
