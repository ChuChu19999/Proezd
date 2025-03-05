import os
import pandas as pd
import re
from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.admin.views.decorators import staff_member_required
from django.db import connection
from django.http import JsonResponse, FileResponse
from .forms import PotokUploadForm, PropuskUploadForm
from datetime import datetime, timedelta
import logging
from difflib import SequenceMatcher
from collections import defaultdict
from functools import lru_cache
import json
import multiprocessing as mp
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

POTOK_TABLE = os.getenv("POSTGRES_TABLE_POTOK")
PROPUSK_TABLE = os.getenv("POSTGRES_TABLE_PROPUSK")
COMPANY_TABLE = os.getenv("POSTGRES_TABLE_COMPANY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1000)
def similar(a, b):
    """Вычисляет схожесть двух строк с учетом форматов российских номеров"""
    logger.info(f"Сравниваем номера {a} и {b}")

    a_pattern = a.replace("?", ".")
    b_pattern = b.replace("?", ".")

    # Если в номере есть ?, считаем его частично распознанным
    if "?" in a or "?" in b:
        logger.info(f"Один из номеров содержит ?, проверяем совпадение по шаблону")
        if re.match(f"^{a_pattern}$", b) or re.match(f"^{b_pattern}$", a):
            logger.info(f"Номера совпадают по шаблону, возвращаем 0.9")
            return 0.9
        logger.info(f"Номера не совпадают по шаблону")

    def get_plate_type(plate):
        if len(plate) < 4:
            return "unknown"

        # Паттерны для стандартных номеров с учетом возможного отсутствия региона
        patterns = {
            r"^[АВЕКМНОРСТУХ?][0-9?]{3}[АВЕКМНОРСТУХ?]{2}([0-9?]{2,3})?$": "standard",
            r"^[0-9?]{4}[АВЕКМНОРСТУХ?]{2}([0-9?]{2,3})?$": "trailer",
            r"^[АВЕКМНОРСТУХ?][0-9?]{3}[АВЕКМНОРСТУХ?]{2}$": "diplomatic",
            r"^[АВЕКМНОРСТУХ?]{2}[0-9?]{5,7}$": "special",
        }

        # Если в номере есть ?, проверяем, что он соответствует хотя бы базовой структуре
        if "?" in plate:
            basic_structure = r"^[АВЕКМНОРСТУХ0-9?]+$"
            if not re.match(basic_structure, plate):
                return "unknown"

            # Проверяем соответствие базовым шаблонам без учета длины
            base_patterns = {
                r"^[АВЕКМНОРСТУХ?][0-9?]+[АВЕКМНОРСТУХ?]{2}": "standard",
                r"^[0-9?]+[АВЕКМНОРСТУХ?]{2}": "trailer",
                r"^[АВЕКМНОРСТУХ?][0-9?]+[АВЕКМНОРСТУХ?]{2}$": "diplomatic",
                r"^[АВЕКМНОРСТУХ?]{2}[0-9?]+$": "special",
            }

            for pattern, type_name in base_patterns.items():
                if re.match(pattern, plate):
                    return type_name

        # Стандартная проверка для номеров без ?
        for pattern, type_name in patterns.items():
            if re.match(pattern, plate):
                return type_name

        return "unknown"

    type_a = get_plate_type(a)
    type_b = get_plate_type(b)
    logger.info(f"Тип номера {a}: {type_a}")
    logger.info(f"Тип номера {b}: {type_b}")

    if type_a == "unknown" or type_b == "unknown":
        logger.info(f"Один из номеров неизвестного типа, возвращаем 0.3")
        return 0.3

    type_weights = {
        "standard": 0.95,
        "trailer": 0.93,
        "diplomatic": 0.94,
        "special": 0.92,
    }

    base_similarity = SequenceMatcher(None, a, b).ratio()
    logger.info(f"Базовая схожесть: {base_similarity:.2%}")

    if type_a == type_b:
        base_similarity = base_similarity * 1.1
        logger.info(f"Типы совпадают, увеличенная схожесть: {base_similarity:.2%}")

    similar_digits = {
        "0": "О",
        "8": "В",
        "3": "З",
    }

    if len(a) == len(b):
        matches = sum(
            1
            for i in range(len(a))
            if a[i] == b[i]
            or a[i] == "?"
            or b[i] == "?"
            or (a[i] in similar_digits and b[i] == similar_digits[a[i]])
            or (b[i] in similar_digits and a[i] == similar_digits[b[i]])
        )
        char_similarity = matches / len(a)
        logger.info(
            f"Посимвольная схожесть: {char_similarity:.2%} ({matches}/{len(a)} символов)"
        )
        base_similarity = max(base_similarity, char_similarity)
        logger.info(f"Итоговая базовая схожесть: {base_similarity:.2%}")

    if a.startswith(b) or b.startswith(a):
        min_len = min(len(a), len(b))
        max_len = max(len(a), len(b))
        weight = type_weights[type_a if len(a) < len(b) else type_b]
        final_similarity = max(
            base_similarity, min_len / max_len * weight + (1 - weight)
        )
        logger.info(
            f"Один номер начинается с другого, финальная схожесть: {final_similarity:.2%}"
        )
        return final_similarity

    logger.info(f"Возвращаем финальную схожесть: {min(base_similarity, 1.0):.2%}")
    return min(base_similarity, 1.0)


def prepare_reference_numbers(numbers):
    by_length = defaultdict(set)
    by_prefix = defaultdict(set)
    for num in numbers:
        length = len(num)
        by_length[length].add(num)
        if len(num) >= 2:
            prefix = num[:2]
            by_prefix[prefix].add(num)
    return by_length, by_prefix


def get_most_similar_number(plate, ref_data, threshold=0.6):
    by_length, by_prefix = ref_data
    plate_len = len(plate)
    possible_lengths = {
        plate_len - 2,
        plate_len - 1,
        plate_len,
        plate_len + 1,
        plate_len + 2,
    }
    candidates = set()

    logger.info(f"Ищем похожие номера для {plate} с порогом {threshold}")
    logger.info(f"Длина номера: {plate_len}")
    logger.info(f"Возможные длины: {possible_lengths}")

    if len(plate) >= 2:
        prefix = plate[:2]
        candidates.update(by_prefix[prefix])
        logger.info(f"Добавлены кандидаты по префиксу {prefix}: {by_prefix[prefix]}")

    for length in possible_lengths:
        candidates.update(by_length[length])
        logger.info(f"Добавлены кандидаты длины {length}: {by_length[length]}")

    if len(candidates) > 100 and len(plate) >= 2:
        prefix = plate[:2]
        candidates = by_prefix[prefix]
        logger.info(
            f"Слишком много кандидатов, оставляем только по префиксу {prefix}: {candidates}"
        )

    best_matches = []
    best_similarity = threshold

    logger.info(f"Всего кандидатов для проверки: {len(candidates)}")
    for ref in candidates:
        if abs(len(ref) - plate_len) > 3:
            logger.info(
                f"Пропускаем {ref} - слишком большая разница в длине ({len(ref)} vs {plate_len})"
            )
            continue

        common_chars = sum(
            1 for a, b in zip(plate, ref) if a == b or a == "?" or b == "?"
        )
        if common_chars / max(len(plate), len(ref)) < threshold:
            logger.info(
                f"Пропускаем {ref} - мало общих символов ({common_chars}/{max(len(plate), len(ref))})"
            )
            continue

        similarity = similar(plate, ref)
        logger.info(f"Проверяем {ref} - схожесть {similarity:.2%}")

        # Если нашли номер с такой же схожестью, добавляем его в список
        if (
            abs(similarity - best_similarity) < 0.0001
        ):  # Используем небольшой допуск для сравнения float
            best_matches.append((ref, similarity))
            logger.info(f"Найден еще один номер с такой же схожестью: {ref}")
        # Если нашли номер с большей схожестью, очищаем список и добавляем новый
        elif similarity > best_similarity:
            best_similarity = similarity
            best_matches = [(ref, similarity)]

    # Если найдено больше одного номера с одинаковой схожестью, возвращаем None
    if len(best_matches) > 1:
        logger.info(
            f"Найдено несколько номеров с одинаковой схожестью {best_similarity:.2%}: {[match[0] for match in best_matches]}"
        )
        return None
    elif best_matches:
        logger.info(
            f"Итоговый результат: {best_matches[0][0]} с схожестью {best_matches[0][1]:.2%}"
        )
        return best_matches[0]
    else:
        logger.info("Похожих номеров не найдено")
        return None


def process_chunk(chunk_data):
    """Обработка части номеров в отдельном процессе"""
    chunk_plates, ref_data, threshold = chunk_data
    results = []

    for plate, dt, potok_id in chunk_plates:
        if not plate:
            continue

        # Преобразуем в строку и приводим к верхнему регистру
        plate = str(plate).upper()
        logger.info(f"Обрабатываем номер: {plate}")

        # Если в номере есть ?, используем более низкий порог и не проверяем точное совпадение
        if "?" in plate:
            logger.info(f"Номер {plate} содержит ?, ищем похожие с порогом 0.5")
            match_result = get_most_similar_number(plate, ref_data, 0.5)
            if match_result:
                ref_num, similarity = match_result
                logger.info(
                    f"Для номера {plate} найден похожий {ref_num} с схожестью {similarity:.2%}"
                )
                # Добавляем только если схожесть >= 88%
                if similarity >= 0.88:
                    results.append(
                        {
                            "id": potok_id,
                            "original": plate,
                            "suggested": ref_num,
                            "similarity": f"{similarity:.2%}",
                            "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                else:
                    logger.info(
                        f"Номер {plate} пропущен, так как схожесть {similarity:.2%} меньше 88%"
                    )
            else:
                logger.info(
                    f"Для номера {plate} не найдено похожих номеров с порогом 0.5"
                )
        else:
            # Проверяем есть ли точное совпадение в эталонных номерах
            exact_match = False
            for numbers in ref_data[0].values():
                if plate in numbers:
                    exact_match = True
                    break

            if not exact_match:
                match_result = get_most_similar_number(plate, ref_data, threshold)
                if match_result:
                    ref_num, similarity = match_result
                    # Добавляем только если схожесть >= 88%
                    if similarity >= 0.88:
                        results.append(
                            {
                                "id": potok_id,
                                "original": plate,
                                "suggested": ref_num,
                                "similarity": f"{similarity:.2%}",
                                "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                    else:
                        logger.info(
                            f"Номер {plate} пропущен, так как схожесть {similarity:.2%} меньше 88%"
                        )

    return results


@staff_member_required
def analyze_numbers(request):
    try:
        logger.info(f"Получен {request.method} запрос на анализ номеров")
        logger.info(f"Content-Type: {request.headers.get('Content-Type')}")
        logger.info(f"X-CSRFToken: {request.headers.get('X-CSRFToken', 'Нет')}")

        if request.method == "GET":
            logger.info("Возвращаем страницу анализа номеров")
            return render(request, "admin/analyze_numbers.html")

        if request.GET.get("sort") == "true":
            results = request.session.get("analysis_results", [])
            if not results:
                logger.warning("Нет сохраненных результатов анализа")
                return JsonResponse(
                    {"error": "Необходимо выполнить анализ заново"}, status=400
                )

            sort_ascending = (
                request.GET.get("sort_ascending", "false").lower() == "true"
            )

            # Сортируем только результаты, без повторного анализа
            results.sort(
                key=lambda x: float(x["similarity"].rstrip("%")),
                reverse=not sort_ascending,
            )
            logger.info(
                f"Результаты отсортированы по {'возрастанию' if sort_ascending else 'убыванию'} процента схожести"
            )

            return JsonResponse({"results": results})

        logger.info("Начинаем анализ номеров...")

        with connection.cursor() as cursor:
            try:
                logger.info("Получаем список эталонных номеров из базы...")
                cursor.execute(
                    f"""
                    SELECT gn
                    FROM {PROPUSK_TABLE}
                    WHERE gn IS NOT NULL
                    AND dateactual IS NULL
                """
                )
                reference_numbers = {row[0].upper() for row in cursor.fetchall()}
                logger.info(f"Получено {len(reference_numbers)} эталонных номеров")
                logger.info(f"Эталонные номера: {', '.join(sorted(reference_numbers))}")

                if not reference_numbers:
                    logger.warning("Не найдено эталонных номеров в базе")
                    return JsonResponse(
                        {"error": "Не найдено эталонных номеров в базе"}, status=400
                    )

                ref_data = prepare_reference_numbers(reference_numbers)
                logger.info(
                    f"Подготовлены индексы для поиска. Длины номеров: {len(ref_data[0])}, Префиксы: {len(ref_data[1])}"
                )

                # Получаем все номера для анализа с дополнительной фильтрацией латинских букв
                logger.info("Получаем номера для анализа из потока...")
                try:
                    cursor.execute(
                        f"""
                        SELECT gosnmr, dt, potok_id
                        FROM {POTOK_TABLE}
                        WHERE gosnmr IS NOT NULL 
                        AND del IS NULL
                        AND gosnmr !~ '[A-Za-z]'
                        ORDER BY dt
                    """
                    )
                except Exception as e:
                    logger.error(
                        f"Ошибка SQL при получении номеров из потока: {str(e)}",
                        exc_info=True,
                    )
                    return JsonResponse(
                        {"error": f"Ошибка при получении номеров из базы: {str(e)}"},
                        status=500,
                    )

                potok_numbers = cursor.fetchall()
                logger.info(f"Получено {len(potok_numbers)} номеров для анализа")
                logger.info(
                    f"Номера для анализа: {', '.join(str(num[0]) for num in potok_numbers[:10])}..."
                )

                if not potok_numbers:
                    logger.warning("Не найдено номеров для анализа")
                    return JsonResponse(
                        {"error": "Не найдено номеров для анализа"}, status=400
                    )

                # Параллельная обработка
                num_processes = mp.cpu_count()
                chunk_size = len(potok_numbers) // num_processes + 1
                chunks = [
                    potok_numbers[i : i + chunk_size]
                    for i in range(0, len(potok_numbers), chunk_size)
                ]

                # Подготовка данных для параллельной обработки
                chunk_data = [(chunk, ref_data, 0.6) for chunk in chunks]

                logger.info(
                    f"Начинаем параллельную обработку используя {num_processes} процессов"
                )

                with mp.Pool(processes=num_processes) as pool:
                    results = []
                    for chunk_results in pool.imap_unordered(process_chunk, chunk_data):
                        results.extend(chunk_results)

                # Сохраняем результаты в сессии для последующей сортировки
                request.session["analysis_results"] = results

                results.sort(
                    key=lambda x: float(x["similarity"].rstrip("%")), reverse=True
                )
                logger.info("Результаты отсортированы по убыванию процента схожести")

                return JsonResponse({"results": results})

            except Exception as e:
                logger.error(
                    f"Ошибка при работе с базой данных: {str(e)}", exc_info=True
                )
                return JsonResponse(
                    {"error": f"Ошибка при работе с базой данных: {str(e)}"}, status=500
                )

    except Exception as e:
        logger.error(f"Критическая ошибка в analyze_numbers: {str(e)}", exc_info=True)
        return JsonResponse({"error": f"Критическая ошибка: {str(e)}"}, status=500)


@staff_member_required
def replace_numbers(request):
    if request.method != "POST":
        return JsonResponse({"error": "Метод не поддерживается"}, status=405)

    try:
        data = json.loads(request.body)
        replacements = data.get("replacements", [])

        with connection.cursor() as cursor:
            for item in replacements:
                cursor.execute(
                    f"""
                    UPDATE {POTOK_TABLE}
                    SET gosnmr = %s
                    WHERE potok_id = %s
                """,
                    (item["suggested"], item["id"]),
                )

            connection.commit()

        return JsonResponse(
            {"success": True, "message": f"Обновлено {len(replacements)} записей"}
        )

    except Exception as e:
        logger.error(f"Ошибка при замене номеров: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)


def process_plate(plate):
    plate = str(plate).upper().strip()

    # Находим последнюю цифру и обрезаем все после нее
    last_digit_match = re.search(r"\d(?!.*\d)", plate)
    if last_digit_match:
        end_pos = last_digit_match.end()
        plate = plate[:end_pos]

    # Проверяем наличие английских букв
    eng_letters = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    if not any(c in eng_letters for c in plate):
        return plate

    en_to_ru = {
        "A": "А",
        "B": "В",
        "E": "Е",
        "K": "К",
        "M": "М",
        "H": "Н",
        "O": "О",
        "P": "Р",
        "C": "С",
        "T": "Т",
        "X": "Х",
        "Y": "У",
    }

    # Паттерны российских номеров
    patterns = [
        r"^[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}$",  # Стандартный
        r"^\d{4}[АВЕКМНОРСТУХ]{2}\d{2,3}$",  # Прицеп
        r"^[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}$",  # Дипломатические
        r"^[АВЕКМНОРСТУХ]{2}\d{5,7}$",  # Спецтранспорт
    ]

    # Заменяем английские буквы на русские для проверки
    test_plate = plate
    for en, ru in en_to_ru.items():
        test_plate = test_plate.replace(en, ru)

    is_russian = any(re.match(pattern, test_plate) for pattern in patterns)

    if is_russian:
        for en, ru in en_to_ru.items():
            plate = plate.replace(en, ru)

    return plate


def get_schema_name():
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_schema()")
        return cursor.fetchone()[0]


@staff_member_required
def upload_potok(request):
    if request.method == "POST":
        form = PotokUploadForm(request.POST, request.FILES)
        if form.is_valid():
            try:
                df = pd.read_excel(request.FILES["file"])

                if len(pd.ExcelFile(request.FILES["file"]).sheet_names) > 1:
                    messages.error(
                        request, "Excel файл должен содержать только один лист"
                    )
                    return redirect("admin:index")

                required_headers = {
                    "A": "Дата фиксации",
                    "B": "Гос. номер",
                    "D": "Приближение/удаление",
                    "E": "Камера",
                }

                for col, header in required_headers.items():
                    if df.columns[ord(col) - ord("A")] != header:
                        messages.error(
                            request,
                            f"Неверный заголовок в столбце {col}. Ожидается: {header}",
                        )
                        return redirect("admin:index")

                df["Дата фиксации"] = pd.to_datetime(df["Дата фиксации"])

                # Получаем даты из формы и убираем информацию о часовом поясе
                date_from = form.cleaned_data["date_from"]
                if date_from.tzinfo:
                    date_from = date_from.replace(tzinfo=None)

                date_to = form.cleaned_data["date_to"]
                if date_to.tzinfo:
                    date_to = date_to.replace(tzinfo=None)
                date_to = date_to.replace(hour=23, minute=59, second=59)

                # Проверяем диапазон дат
                if not all(
                    (date_from <= date <= date_to) for date in df["Дата фиксации"]
                ):
                    messages.error(
                        request, "В файле есть записи вне выбранного диапазона дат"
                    )
                    return redirect("admin:index")

                df["Гос. номер"] = df["Гос. номер"].apply(process_plate)
                df["Приближение/удаление"] = df["Приближение/удаление"].str.upper()

                schema_name = get_schema_name()

                # Получаем список индексов
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT schemaname, tablename, indexname, indexdef 
                        FROM pg_indexes 
                        WHERE tablename = '{POTOK_TABLE}' 
                        AND schemaname = %s
                    """,
                        [schema_name],
                    )
                    indexes = cursor.fetchall()

                # Удаляем индексы
                with connection.cursor() as cursor:
                    for schema, table, index_name, _ in indexes:
                        cursor.execute(f"DROP INDEX IF EXISTS {schema}.{index_name}")

                # Получаем максимальный potok_id
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT COALESCE(MAX(potok_id), 0) FROM {POTOK_TABLE}"
                    )
                    max_potok_id = cursor.fetchone()[0]

                # Подготавливаем данные для вставки
                records = []
                for idx, row in df.iterrows():
                    records.append(
                        {
                            "gosnmr": row["Гос. номер"],
                            "camera": row["Камера"],
                            "direction": row["Приближение/удаление"],
                            "filename": request.FILES["file"].name,
                            "date_load_bgn": date_from,
                            "date_load_end": date_to,
                            "dt": row["Дата фиксации"],
                            "potok_id": max_potok_id + idx + 1,
                        }
                    )

                with connection.cursor() as cursor:
                    cursor.execute("SET timezone TO 'Asia/Yekaterinburg'")
                    for record in records:
                        cursor.execute(
                            f"""
                            INSERT INTO {POTOK_TABLE} (
                                potok_id, gosnmr, dt, camera, direction, filename,
                                date_load_bgn, date_load_end
                            ) VALUES (
                                %(potok_id)s, 
                                %(gosnmr)s,
                                %(dt)s,
                                %(camera)s,
                                %(direction)s,
                                %(filename)s,
                                %(date_load_bgn)s,
                                %(date_load_end)s
                            )
                        """,
                            record,
                        )

                # Восстанавливаем индексы
                with connection.cursor() as cursor:
                    for schema, table, index_name, index_def in indexes:
                        # Проверяем существование индекса перед созданием
                        cursor.execute(
                            f"""
                            SELECT 1 FROM pg_indexes 
                            WHERE schemaname = %s 
                            AND tablename = %s 
                            AND indexname = %s
                        """,
                            [schema, table, index_name],
                        )
                        if not cursor.fetchone():
                            cursor.execute(index_def)

                messages.success(request, "Файл успешно обработан")
                return redirect("admin:index")

            except Exception as e:
                messages.error(request, f"Ошибка при обработке файла: {str(e)}")
                return redirect("admin:index")
    else:
        form = PotokUploadForm()

    return render(request, "admin/upload_potok.html", {"form": form})


def get_next_id(cursor, table, id_field):
    cursor.execute(f"SELECT COALESCE(MAX({id_field}), 0) FROM {table}")
    return cursor.fetchone()[0] + 1


def get_next_companynr(cursor):
    cursor.execute(f"SELECT COALESCE(MAX(companynr), 0) FROM {COMPANY_TABLE}")
    return cursor.fetchone()[0] + 1


def process_company(cursor, company_str):
    # Пытаемся извлечь companynr из строки (число перед точкой)
    match = re.match(r"^(\d+)\.(.+)$", company_str)

    if match:
        companynr = int(match.group(1))
        company_name = match.group(2).strip()

        # Проверяем существующую запись
        cursor.execute(
            """
            SELECT company_id, company 
            FROM {COMPANY_TABLE}
            WHERE companynr = %s AND del = 0 
            ORDER BY company_id DESC 
            LIMIT 1
        """,
            [companynr],
        )
        existing = cursor.fetchone()

        if existing:
            company_id, existing_name = existing

            if existing_name != company_name:
                # Обновляем dateactual для старой записи
                cursor.execute(
                    """
                    UPDATE {COMPANY_TABLE} 
                    SET dateactual = CURRENT_DATE 
                    WHERE company_id = %s
                """,
                    [company_id],
                )

                # Создаем новую запись
                new_company_id = get_next_id(cursor, "company", "company_id")
                cursor.execute(
                    """
                    INSERT INTO {COMPANY_TABLE} (company_id, company, del, pid, companynr)
                    VALUES (%s, %s, 0, %s, %s)
                """,
                    [new_company_id, company_name, company_id, companynr],
                )

                return new_company_id
            else:
                return company_id
        else:
            # Если нет записи с таким companynr, берем следующий доступный
            next_companynr = get_next_companynr(cursor)
            new_company_id = get_next_id(cursor, "company", "company_id")
            cursor.execute(
                """
                INSERT INTO {COMPANY_TABLE} (company_id, company, del, companynr)
                VALUES (%s, %s, 0, %s)
            """,
                [new_company_id, company_name, next_companynr],
            )
            return new_company_id
    else:
        # Если нет номера, создаем запись с новым companynr
        next_companynr = get_next_companynr(cursor)
        new_company_id = get_next_id(cursor, "company", "company_id")
        cursor.execute(
            """
            INSERT INTO {COMPANY_TABLE} (company_id, company, del, companynr)
            VALUES (%s, %s, 0, %s)
        """,
            [new_company_id, company_str.strip(), next_companynr],
        )
        return new_company_id


@staff_member_required
def upload_propusk(request):
    if request.method == "POST":
        form = PropuskUploadForm(request.POST, request.FILES)
        if form.is_valid():
            try:
                df = pd.read_excel(request.FILES["file"])
                propusk_type = form.cleaned_data["propusk_type"]

                if propusk_type == "kronos":
                    # Проверяем заголовки для базы Кронос
                    required_headers = {
                        "C": "№ ПРОПУСКА",
                        "K": "автомобиль",
                        "L": "Гос.номер",
                        "N": "Разрешенная макс. масса (КГ)ТС",
                        "P": "Срок действия (с..)",
                        "Q": "Срок действия (по..)",
                        "T": "Продлён до",
                        "V": "Комментарий",
                        "W": "Договорные отношения",
                    }

                    for col, header in required_headers.items():
                        if df.columns[ord(col) - ord("A")] != header:
                            messages.error(
                                request,
                                f"Неверный заголовок в столбце {col}. Ожидается: {header}",
                            )
                            return redirect("admin:index")

                    # Преобразуем даты
                    date_columns = [
                        "Срок действия (с..)",
                        "Срок действия (по..)",
                        "Продлён до",
                    ]
                    for col in date_columns:
                        df[col] = pd.to_datetime(
                            df[col], format="%d.%m.%Y", errors="coerce"
                        )

                    with connection.cursor() as cursor:
                        # Получаем следующий propusk_id
                        next_propusk_id = get_next_id(cursor, "propusk", "propusk_id")

                        for idx, row in df.iterrows():
                            # Обработка company_id
                            company_id = process_company(
                                cursor, str(row["От кого письмо"])
                            )

                            # Преобразуем NaT (Not a Time) в None для SQL
                            dateb = (
                                row["Срок действия (с..)"].date()
                                if pd.notna(row["Срок действия (с..)"])
                                else None
                            )
                            datee = (
                                row["Срок действия (по..)"].date()
                                if pd.notna(row["Срок действия (по..)"])
                                else None
                            )
                            prodlen = (
                                row["Продлён до"].date()
                                if pd.notna(row["Продлён до"])
                                else None
                            )

                            # Обработка комментария
                            coment = (
                                row["Комментарий"]
                                if pd.notna(row["Комментарий"])
                                else None
                            )

                            # Вставляем запись в propusk
                            cursor.execute(
                                f"""
                                INSERT INTO {PROPUSK_TABLE} (
                                    propusk_id, gn, company_id, dateb, datee, num,
                                    contractrelationship, tct_id, mass, marka,
                                    prodlen, coment
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """,
                                [
                                    next_propusk_id + idx,
                                    process_plate(
                                        str(row["Гос.номер"]).replace(" ", "")
                                    ),
                                    company_id,
                                    dateb,
                                    datee,
                                    row["№ ПРОПУСКА"],
                                    row["Договорные отношения"],
                                    2,  # tct_id всегда 2 для Кронос
                                    row["Разрешенная макс. масса (КГ)ТС"],
                                    row["автомобиль"],
                                    prodlen,
                                    coment,
                                ],
                            )

                elif propusk_type == "razoviy":
                    # Проверяем заголовки для разовых пропусков
                    required_headers = {
                        "A": "Контрагент",
                        "B": "Госномер",
                        "C": "Дата начала проезда",
                        "D": "Дата окончания проезда",
                        "E": "Марка",
                        "F": "Масса авто",
                        "G": "Номер пропуска",
                        "H": "Зона покрытия",
                        "I": "Договорные отношения",
                        "J": "Продлен до",
                        "K": "Комментарий",
                    }

                    for col, header in required_headers.items():
                        if df.columns[ord(col) - ord("A")] != header:
                            messages.error(
                                request,
                                f"Неверный заголовок в столбце {col}. Ожидается: {header}",
                            )
                            return redirect("admin:index")

                    # Пропускаем первые две строки
                    df = df.iloc[1:]

                    # Преобразуем даты
                    date_columns = [
                        "Дата начала проезда",
                        "Дата окончания проезда",
                        "Продлен до",
                    ]
                    for col in date_columns:
                        df[col] = pd.to_datetime(
                            df[col], format="%d.%m.%Y", errors="coerce"
                        )

                    with connection.cursor() as cursor:
                        # Получаем следующий propusk_id
                        next_propusk_id = get_next_id(cursor, "propusk", "propusk_id")

                        # Обрабатываем каждую строку
                        for idx, row in df.iterrows():
                            # Обработка company_id
                            company_id = process_company(cursor, str(row["Контрагент"]))

                            # Преобразуем NaT (Not a Time) в None для SQL
                            dateb = (
                                row["Дата начала проезда"].date()
                                if pd.notna(row["Дата начала проезда"])
                                else None
                            )
                            datee = (
                                row["Дата окончания проезда"].date()
                                if pd.notna(row["Дата окончания проезда"])
                                else None
                            )
                            prodlen = (
                                row["Продлен до"].date()
                                if pd.notna(row["Продлен до"])
                                else None
                            )

                            # Обработка комментария
                            coment = (
                                row["Комментарий"]
                                if pd.notna(row["Комментарий"])
                                else None
                            )

                            # Обработка договорных отношений (удаление номера и точки в начале)
                            contractrelationship = str(row["Договорные отношения"])
                            if pd.notna(contractrelationship):
                                # Удаляем число и точку в начале
                                contractrelationship = re.sub(
                                    r"^\d+\.", "", contractrelationship
                                ).strip()
                            else:
                                contractrelationship = None

                            # Обработка госномера
                            gn = process_plate(str(row["Госномер"]).replace(" ", ""))

                            # Вставляем запись в propusk
                            cursor.execute(
                                f"""
                                INSERT INTO {PROPUSK_TABLE} (
                                    propusk_id, gn, company_id, dateb, datee, num,
                                    contractrelationship, tct_id, mass, marka,
                                    prodlen, coment
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """,
                                [
                                    next_propusk_id + idx,
                                    gn,
                                    company_id,
                                    dateb,
                                    datee,
                                    row["Номер пропуска"],
                                    contractrelationship,
                                    3,  # tct_id = 3 для разовых пропусков
                                    row["Масса авто"],
                                    row["Марка"],
                                    prodlen,
                                    coment,
                                ],
                            )

                elif propusk_type == "gdya":
                    # Проверяем заголовки для списка ГДЯ
                    required_headers = {
                        "A": "№ пропуска",
                        "C": "Транспортное средство",
                        "D": "Гос. номер",
                        "E": "Получатель пропуска",
                        "G": "Срок действия",
                        "I": "Дата оформления",
                    }

                    for col, header in required_headers.items():
                        if df.columns[ord(col) - ord("A")] != header:
                            messages.error(
                                request,
                                f"Неверный заголовок в столбце {col}. Ожидается: {header}",
                            )
                            return redirect("admin:index")

                    # Преобразуем даты
                    date_columns = ["Срок действия", "Дата оформления"]
                    for col in date_columns:
                        df[col] = pd.to_datetime(
                            df[col], format="%d.%m.%Y", errors="coerce"
                        )

                    with connection.cursor() as cursor:
                        # Получаем следующий propusk_id
                        next_propusk_id = get_next_id(cursor, "propusk", "propusk_id")

                        # Получаем company_id для ГДЯ (companynr = 200)
                        cursor.execute(
                            f"""
                            SELECT company_id 
                            FROM {COMPANY_TABLE} 
                            WHERE companynr = 200 
                            AND dateactual IS NULL
                            ORDER BY company_id DESC 
                            LIMIT 1
                        """
                        )
                        gdya_company = cursor.fetchone()
                        if not gdya_company:
                            messages.error(
                                request,
                                "Не найдена запись компании ГДЯ (companynr = 200)",
                            )
                            return redirect("admin:index")
                        company_id = gdya_company[0]

                        # Обрабатываем каждую строку
                        for idx, row in df.iterrows():
                            # Преобразуем NaT (Not a Time) в None для SQL
                            dateb = (
                                row["Дата оформления"].date()
                                if pd.notna(row["Дата оформления"])
                                else None
                            )
                            datee = (
                                row["Срок действия"].date()
                                if pd.notna(row["Срок действия"])
                                else None
                            )

                            # Обработка госномера
                            gn = process_plate(str(row["Гос. номер"]).replace(" ", ""))

                            # Вставляем запись в propusk
                            cursor.execute(
                                f"""
                                INSERT INTO {PROPUSK_TABLE} (
                                    propusk_id, gn, company_id, dateb, datee, num,
                                    contractrelationship, tct_id, mass, marka
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """,
                                [
                                    next_propusk_id + idx,
                                    gn,
                                    company_id,
                                    dateb,
                                    datee,
                                    row["№ пропуска"],
                                    row["Получатель пропуска"],
                                    5,  # tct_id = 5 для списка ГДЯ
                                    0,  # mass = 0
                                    row["Транспортное средство"],
                                ],
                            )

                messages.success(request, "Файл успешно обработан")
                return redirect("admin:index")

            except Exception as e:
                messages.error(request, f"Ошибка при обработке файла: {str(e)}")
                return redirect("admin:index")
    else:
        form = PropuskUploadForm()

    return render(request, "admin/upload_propusk.html", {"form": form})


def get_vehicle_category(mass):
    """Определяет категорию ТС на основе массы"""
    if mass is None:
        return None
    if mass <= 3500:
        return 1
    elif mass <= 10000:
        return 2
    elif mass <= 25000:
        return 3
    else:
        return 4


@staff_member_required
def generate_report(request):
    if request.method == "POST":
        try:
            # Получаем дату и тарифы из формы
            report_date = datetime.strptime(request.POST.get("report_date"), "%Y-%m")
            month_name = report_date.strftime("%B_%Y")

            month_translations = {
                "January": "Январь",
                "February": "Февраль",
                "March": "Март",
                "April": "Апрель",
                "May": "Май",
                "June": "Июнь",
                "July": "Июль",
                "August": "Август",
                "September": "Сентябрь",
                "October": "Октябрь",
                "November": "Ноябрь",
                "December": "Декабрь",
            }

            # Переводим название месяца на русский
            month_eng = report_date.strftime("%B")
            month_rus = month_translations[month_eng]
            sheet_name = f"{month_rus}_{report_date.year}"

            # Получаем тарифы
            tariffs = {
                1: float(request.POST.get("tariff_1", 0)),
                2: float(request.POST.get("tariff_2", 0)),
                3: float(request.POST.get("tariff_3", 0)),
                4: float(request.POST.get("tariff_4", 0)),
            }

            # Создаем два Excel файла
            wb = Workbook()
            wb.remove(wb.active)  # Удаляем стандартный лист
            ws = wb.create_sheet(title=sheet_name)

            wb_simple = Workbook()
            wb_simple.remove(wb_simple.active)  # Удаляем стандартный лист
            ws_simple = wb_simple.create_sheet(title=sheet_name)

            # Создаем стили
            header_font = Font(bold=True)
            center_alignment = Alignment(
                horizontal="center", vertical="center", wrap_text=True
            )
            red_fill = PatternFill(
                start_color="FFFF0000", end_color="FFFF0000", fill_type="solid"
            )
            thin_border = Border(
                left=Side(style="thin"),
                right=Side(style="thin"),
                top=Side(style="thin"),
                bottom=Side(style="thin"),
            )

            # Устанавливаем заголовки для полного отчета
            headers = [
                "Гос. номер",
                "№ фиксации",
                "Камера",
                "Дата фиксации",
                "Перевозчик",
                "Выбранный перевозчик",
                "Категория",
                "Тариф",
                "Сумма",
            ]

            # Устанавливаем заголовки для упрощенного отчета
            headers_simple = [
                "Гос. номер",
                "Кол-во фиксаций",
                "Перевозчик",
                "Выбранный перевозчик",
                "Сумма",
            ]

            # Заполняем заголовки для полного отчета
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col)
                cell.value = header
                cell.font = header_font
                cell.alignment = center_alignment
                cell.border = thin_border

            # Заполняем заголовки для упрощенного отчета
            for col, header in enumerate(headers_simple, 1):
                cell = ws_simple.cell(row=1, column=col)
                cell.value = header
                cell.font = header_font
                cell.alignment = center_alignment
                cell.border = thin_border

            # Устанавливаем ширину столбцов для полного отчета
            column_widths = {
                1: 15,  # Гос. номер
                2: 12,  # № фиксации
                3: 20,  # Фиксация на камере
                4: 20,  # Дата фиксации
                5: 30,  # Перевозчик
                6: 30,  # Выбранный перевозчик
                7: 15,  # Категория
                8: 15,  # Тариф
                9: 15,  # Сумма
            }

            # Устанавливаем ширину столбцов для упрощенного отчета
            column_widths_simple = {
                1: 15,  # Гос. номер
                2: 15,  # Кол-во фиксаций
                3: 30,  # Перевозчик
                4: 30,  # Выбранный перевозчик
                5: 15,  # Сумма
            }

            for col, width in column_widths.items():
                ws.column_dimensions[get_column_letter(col)].width = width

            for col, width in column_widths_simple.items():
                ws_simple.column_dimensions[get_column_letter(col)].width = width

            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT DISTINCT gn 
                    FROM {PROPUSK_TABLE} 
                    WHERE gn IS NOT NULL
                """
                )
                valid_numbers = {row[0].upper() for row in cursor.fetchall()}

                # Получаем данные о массе для действующих пропусков и информацию о компаниях
                cursor.execute(
                    f"""
                    SELECT p.gn, p.mass, p.company_id, p.tct_id, c.company, p.dateb, p.datee
                    FROM {PROPUSK_TABLE} p
                    LEFT JOIN {COMPANY_TABLE} c ON p.company_id = c.company_id
                    WHERE p.gn IS NOT NULL 
                    AND p.dateactual IS NULL
                    ORDER BY p.gn, p.tct_id, c.company
                """
                )
                vehicle_masses = {}
                vehicle_companies = defaultdict(lambda: defaultdict(list))
                vehicle_tct_ids = defaultdict(set)
                vehicle_companies_by_tct = defaultdict(lambda: defaultdict(list))
                for row in cursor.fetchall():
                    gn, mass, company_id, tct_id, company, dateb, datee = row
                    gn = gn.upper()
                    vehicle_masses[gn] = mass
                    if company:  # Добавляем компанию только если она существует
                        vehicle_companies[gn]["dates"].append((dateb, datee))
                        vehicle_companies[gn]["companies"].append(company)
                    if tct_id:  # Сохраняем tct_id и компании для каждого tct_id
                        vehicle_tct_ids[gn].add(tct_id)
                        vehicle_companies_by_tct[gn][tct_id].append(
                            {"company": company, "dateb": dateb, "datee": datee}
                        )

                # Получаем данные потока за выбранный месяц
                cursor.execute(
                    f"""
                    WITH RankedRecords AS (
                        SELECT 
                            gosnmr,
                            camera,
                            direction,
                            dt,
                            LAG(dt) OVER (PARTITION BY gosnmr ORDER BY dt) as prev_dt
                        FROM {POTOK_TABLE}
                        WHERE 
                            EXTRACT(YEAR FROM dt) = %s 
                            AND EXTRACT(MONTH FROM dt) = %s
                            AND del IS NULL
                        ORDER BY gosnmr, dt
                    )
                    SELECT * FROM RankedRecords
                """,
                    [report_date.year, report_date.month],
                )

                current_row = 2
                current_row_simple = 2
                current_number = None
                fixation_count = 0
                merge_start_row = 2
                merge_start_row_simple = 2

                for row in cursor.fetchall():
                    gosnmr, camera, direction, dt, prev_dt = row

                    # Пропускаем запись если прошло менее 24 часов с предыдущей
                    if prev_dt and (dt - prev_dt) < timedelta(hours=24):
                        continue

                    # Если новый номер, объединяем предыдущие ячейки и обновляем счетчик
                    if gosnmr != current_number:
                        # Объединяем ячейки для предыдущего номера в полном отчете
                        if current_number and current_row > merge_start_row:
                            for col in [1, 5, 6, 7, 8, 9]:  # Столбцы для объединения
                                ws.merge_cells(
                                    start_row=merge_start_row,
                                    start_column=col,
                                    end_row=current_row - 1,
                                    end_column=col,
                                )
                                # Устанавливаем выравнивание для объединенной ячейки
                                merged_cell = ws.cell(row=merge_start_row, column=col)
                                merged_cell.alignment = Alignment(
                                    horizontal="center", vertical="center"
                                )

                            # Добавляем запись в упрощенный отчет (только максимальная фиксация)
                            ws_simple.cell(row=current_row_simple, column=1).value = (
                                current_number
                            )
                            ws_simple.cell(row=current_row_simple, column=2).value = (
                                fixation_count
                            )

                            # Копируем значения из полного отчета
                            for src_col, dst_col in [(5, 3), (6, 4), (9, 5)]:
                                cell_value = ws.cell(
                                    row=merge_start_row, column=src_col
                                ).value
                                ws_simple.cell(
                                    row=current_row_simple, column=dst_col
                                ).value = cell_value

                            # Форматируем ячейки в упрощенном отчете
                            for col in range(1, 6):
                                cell = ws_simple.cell(
                                    row=current_row_simple, column=col
                                )
                                cell.alignment = center_alignment
                                cell.border = thin_border
                                if (
                                    col == 1
                                    and current_number.upper() not in valid_numbers
                                ):
                                    cell.fill = red_fill

                            current_row_simple += 1

                        current_number = gosnmr
                        fixation_count = 1
                        merge_start_row = current_row
                    else:
                        fixation_count += 1

                    # Записываем данные
                    # Гос. номер (только для первой строки номера)
                    if merge_start_row == current_row:
                        cell = ws.cell(row=current_row, column=1)
                        cell.value = gosnmr
                        cell.alignment = center_alignment
                        cell.border = thin_border

                        upper_gosnmr = gosnmr.upper()
                        if upper_gosnmr not in valid_numbers:
                            cell.fill = red_fill

                        # Создаем ячейку категории с форматированием
                        category_cell = ws.cell(
                            row=current_row, column=7
                        )  # Столбец категории
                        category_cell.alignment = center_alignment
                        category_cell.border = thin_border

                        # Создаем ячейку тарифа с форматированием
                        tariff_cell = ws.cell(
                            row=current_row, column=8
                        )  # Столбец тарифа
                        tariff_cell.alignment = center_alignment
                        tariff_cell.border = thin_border

                        # Создаем ячейку суммы с форматированием
                        sum_cell = ws.cell(row=current_row, column=9)  # Столбец суммы
                        sum_cell.alignment = center_alignment
                        sum_cell.border = thin_border

                        # Заполняем значение категории, компаний, выбранного перевозчика, тарифа и суммы если номер есть в действующих пропусках
                        if upper_gosnmr in vehicle_masses:
                            mass = vehicle_masses[upper_gosnmr]
                            category = get_vehicle_category(mass)
                            category_cell.value = category

                            # Заполняем список компаний
                            companies_cell = ws.cell(
                                row=current_row, column=5
                            )  # Столбец перевозчика
                            valid_companies = []
                            if upper_gosnmr in vehicle_companies:
                                for i, (dateb, datee) in enumerate(
                                    vehicle_companies[upper_gosnmr]["dates"]
                                ):
                                    # Проверяем, попадает ли дата фиксации в период действия пропуска
                                    if (dateb is None or dt.date() >= dateb) and (
                                        datee is None or dt.date() <= datee
                                    ):
                                        valid_companies.append(
                                            vehicle_companies[upper_gosnmr][
                                                "companies"
                                            ][i]
                                        )

                            companies_cell.value = ", ".join(valid_companies)
                            companies_cell.alignment = center_alignment
                            companies_cell.border = thin_border

                            # Заполняем выбранного перевозчика (с наименьшим tct_id)
                            selected_companies_cell = ws.cell(
                                row=current_row, column=6
                            )  # Столбец выбранного перевозчика
                            if upper_gosnmr in vehicle_companies_by_tct:
                                valid_companies_by_tct = defaultdict(list)
                                # Проходим по всем tct_id и компаниям
                                for tct_id, companies in vehicle_companies_by_tct[
                                    upper_gosnmr
                                ].items():
                                    for company_data in companies:
                                        dateb = company_data["dateb"]
                                        datee = company_data["datee"]
                                        # Проверяем, попадает ли дата фиксации в период действия пропуска
                                        if (dateb is None or dt.date() >= dateb) and (
                                            datee is None or dt.date() <= datee
                                        ):
                                            valid_companies_by_tct[tct_id].append(
                                                company_data["company"]
                                            )

                                # Если есть действующие компании, берем те, что с минимальным tct_id
                                if valid_companies_by_tct:
                                    min_tct_id = min(valid_companies_by_tct.keys())
                                    selected_companies_cell.value = ", ".join(
                                        valid_companies_by_tct[min_tct_id]
                                    )

                            selected_companies_cell.alignment = center_alignment
                            selected_companies_cell.border = thin_border

                            # Заполняем тариф соответствующий категории
                            if category in tariffs:
                                tariff = tariffs[category]
                                tariff_cell.value = f"{tariff:.2f}"

                                # Считаем количество фиксаций для этого номера
                                cursor.execute(
                                    f"""
                                    WITH RankedRecords AS (
                                        SELECT 
                                            dt,
                                            LAG(dt) OVER (ORDER BY dt) as prev_dt
                                        FROM {POTOK_TABLE}
                                        WHERE 
                                            gosnmr = %s
                                            AND EXTRACT(YEAR FROM dt) = %s 
                                            AND EXTRACT(MONTH FROM dt) = %s
                                            AND del IS NULL
                                        ORDER BY dt
                                    )
                                    SELECT COUNT(*) 
                                    FROM RankedRecords
                                    WHERE prev_dt IS NULL OR dt - prev_dt >= interval '24 hours'
                                """,
                                    [gosnmr, report_date.year, report_date.month],
                                )

                                total_fixations = cursor.fetchone()[0]
                                # Рассчитываем сумму и записываем в ячейку
                                total_sum = tariff * total_fixations
                                sum_cell.value = f"{total_sum:.2f}"

                        # Добавляем пустые значения для остальных новых столбцов
                        for col in [
                            5,
                            6,
                        ]:  # Пропускаем столбцы категории (7) и тарифа (8)
                            cell = ws.cell(row=current_row, column=col)
                            cell.alignment = center_alignment
                            cell.border = thin_border

                    # № фиксации
                    cell = ws.cell(row=current_row, column=2)
                    cell.value = fixation_count
                    cell.alignment = center_alignment
                    cell.border = thin_border

                    # Фиксация на камере
                    cell = ws.cell(row=current_row, column=3)
                    cell.value = f"{camera} ({direction})"
                    cell.alignment = center_alignment
                    cell.border = thin_border

                    # Дата фиксации
                    cell = ws.cell(row=current_row, column=4)
                    cell.value = dt.strftime("%d.%m.%Y %H:%M:%S")
                    cell.alignment = center_alignment
                    cell.border = thin_border

                    current_row += 1

                # Объединяем ячейки для последнего номера в полном отчете
                if current_number and current_row > merge_start_row:
                    for col in [1, 5, 6, 7, 8, 9]:  # Столбцы для объединения
                        ws.merge_cells(
                            start_row=merge_start_row,
                            start_column=col,
                            end_row=current_row - 1,
                            end_column=col,
                        )
                        # Устанавливаем выравнивание для объединенной ячейки
                        merged_cell = ws.cell(row=merge_start_row, column=col)
                        merged_cell.alignment = Alignment(
                            horizontal="center", vertical="center"
                        )

                # Добавляем последнюю запись в упрощенный отчет
                ws_simple.cell(row=current_row_simple, column=1).value = current_number
                ws_simple.cell(row=current_row_simple, column=2).value = fixation_count

                # Копируем значения из полного отчета
                for src_col, dst_col in [(5, 3), (6, 4), (9, 5)]:
                    cell_value = ws.cell(row=merge_start_row, column=src_col).value
                    ws_simple.cell(row=current_row_simple, column=dst_col).value = (
                        cell_value
                    )

                # Форматируем ячейки в упрощенном отчете
                for col in range(1, 6):
                    cell = ws_simple.cell(row=current_row_simple, column=col)
                    cell.alignment = center_alignment
                    cell.border = thin_border
                    if col == 1 and current_number.upper() not in valid_numbers:
                        cell.fill = red_fill

            # Сохраняем файлы
            filename = f'Анализ_потока_{report_date.strftime("%m_%Y")}.xlsx'
            filename_simple = (
                f'Анализ_потока_краткий_{report_date.strftime("%m_%Y")}.xlsx'
            )

            filepath = os.path.join("media", filename)
            filepath_simple = os.path.join("media", filename_simple)

            os.makedirs("media", exist_ok=True)
            wb.save(filepath)
            wb_simple.save(filepath_simple)

            try:
                # Создаем ZIP-архив с обоими файлами
                zip_filename = f'Анализ_потока_{report_date.strftime("%m_%Y")}.zip'
                zip_filepath = os.path.join("media", zip_filename)

                import zipfile

                with zipfile.ZipFile(zip_filepath, "w") as zipf:
                    zipf.write(filepath, os.path.basename(filepath))
                    zipf.write(filepath_simple, os.path.basename(filepath_simple))

                # Отправляем ZIP-архив пользователю
                response = FileResponse(
                    open(zip_filepath, "rb"),
                    content_type="application/zip",
                    as_attachment=True,
                    filename=zip_filename,
                )

                # Добавляем callback для удаления файлов после отправки
                def cleanup():
                    for f in [filepath, filepath_simple, zip_filepath]:
                        if os.path.exists(f):
                            os.remove(f)
                            logger.info(f"Временный файл {f} удален")

                response._resource_closers.append(cleanup)

                return response

            except Exception as e:
                # В случае ошибки удаляем все созданные файлы
                for f in [filepath, filepath_simple, zip_filepath]:
                    if os.path.exists(f):
                        os.remove(f)
                        logger.info(f"Временный файл {f} удален после ошибки")
                messages.error(request, f"Ошибка при формировании отчета: {str(e)}")
                return redirect("admin:index")

        except Exception as e:
            # В случае ошибки также проверяем и удаляем файлы
            for f in (
                [filepath, filepath_simple, zip_filepath]
                if "filepath" in locals()
                else []
            ):
                if os.path.exists(f):
                    os.remove(f)
                    logger.info(f"Временный файл {f} удален после ошибки")
            messages.error(request, f"Ошибка при формировании отчета: {str(e)}")
            return redirect("admin:index")

    else:
        # Добавляем значения по умолчанию в контекст
        context = {
            "default_date": datetime.now().strftime("%Y-%m"),
            "default_tariffs": {
                "tariff_1": "1410,28",
                "tariff_2": "2820,57",
                "tariff_3": "5641,13",
                "tariff_4": "9025,81",
            },
        }
        return render(request, "admin/generate_report.html", context)
