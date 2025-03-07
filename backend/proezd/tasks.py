from celery import shared_task
import logging
from django.db import connection
from datetime import datetime
import json
import os
from difflib import SequenceMatcher
from collections import defaultdict
from functools import lru_cache
import re

POTOK_TABLE = os.getenv("POSTGRES_TABLE_POTOK")
PROPUSK_TABLE = os.getenv("POSTGRES_TABLE_PROPUSK")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


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
            logger.info(f"Новый лучший кандидат: {ref} с схожестью {similarity:.2%}")

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


def process_numbers(numbers, ref_data, threshold=0.6):
    """Обработка номеров"""
    results = []
    total = len(numbers)
    batch_size = 500  # Размер пакета для обработки
    processed = 0

    while processed < total:
        batch = numbers[processed : processed + batch_size]
        batch_results = []

        for idx, (potok_id, plate, dt) in enumerate(batch, 1):
            try:
                if not plate:
                    continue

                if idx % 10 == 0:
                    logger.info(f"Обработано {processed + idx}/{total} номеров")

                # Преобразуем в строку и приводим к верхнему регистру
                plate = str(plate).upper()
                logger.info(f"Обрабатываем номер: {plate}")

                # Создаем множество эталонных номеров для быстрой проверки
                all_ref_numbers = set()
                for numbers in ref_data[0].values():
                    all_ref_numbers.update(numbers)

                # Быстрая проверка на точное совпадение
                if plate in all_ref_numbers:
                    logger.info(f"Найдено точное совпадение для {plate}")
                    continue

                # Если в номере есть ?, используем более низкий порог
                if "?" in plate:
                    logger.info(f"Номер {plate} содержит ?, ищем похожие с порогом 0.5")
                    match_result = get_most_similar_number(plate, ref_data, 0.5)
                else:
                    match_result = get_most_similar_number(plate, ref_data, threshold)

                if match_result:
                    ref_num, similarity = match_result
                    # Добавляем только если схожесть >= 88%
                    if similarity >= 0.88:
                        batch_results.append(
                            {
                                "id": potok_id,
                                "original": plate,
                                "suggested": ref_num,
                                "similarity": f"{similarity:.2%}",
                                "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "skipped": False,
                            }
                        )
                        logger.info(
                            f"Для номера {plate} найден похожий {ref_num} с схожестью {similarity:.2%}"
                        )
                    else:
                        logger.info(
                            f"Номер {plate} пропущен, так как схожесть {similarity:.2%} меньше 88%"
                        )
                else:
                    logger.info(f"Для номера {plate} не найдено похожих номеров")

            except Exception as e:
                logger.error(
                    f"Ошибка при обработке номера {plate}: {str(e)}", exc_info=True
                )
                continue

        results.extend(batch_results)
        processed += len(batch)
        logger.info(
            f"Обработано {processed}/{total} записей ({(processed/total*100):.1f}%)"
        )

    return results


@shared_task
def analyze_numbers_task():
    """
    Ежедневная задача анализа номеров.
    Запускается в 5 утра каждый день.
    """
    current_time = datetime.now()
    logger.info(f"Текущее время запуска задачи: {current_time}")

    try:
        logger.info("Начинаем ежедневный анализ номеров...")

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

                if not reference_numbers:
                    logger.warning("Не найдено эталонных номеров в базе")
                    return

                ref_data = prepare_reference_numbers(reference_numbers)
                logger.info(
                    f"Подготовлены индексы для поиска. Длины номеров: {len(ref_data[0])}, Префиксы: {len(ref_data[1])}"
                )

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {POTOK_TABLE}
                    WHERE gosnmr IS NOT NULL 
                    AND del IS NULL
                    AND gosnmr !~ '[A-Za-z]'
                """
                )
                total_records = cursor.fetchone()[0]
                logger.info(f"Всего записей для обработки: {total_records}")

                BATCH_SIZE = 500
                processed_records = 0
                all_results = []

                while processed_records < total_records:
                    try:
                        cursor.execute(
                            f"""
                            SELECT potok_id, gosnmr, dt
                            FROM {POTOK_TABLE}
                            WHERE gosnmr IS NOT NULL 
                            AND del IS NULL
                            AND gosnmr !~ '[A-Za-z]'
                            ORDER BY dt
                            LIMIT {BATCH_SIZE}
                            OFFSET {processed_records}
                        """
                        )

                        batch_data = cursor.fetchall()
                        if not batch_data:
                            logger.info("Достигнут конец данных")
                            break

                        logger.info(
                            f"Начало обработки пакета {processed_records + 1}-{processed_records + len(batch_data)} из {total_records} записей"
                        )

                        batch_results = process_numbers(batch_data, ref_data, 0.6)
                        all_results.extend(batch_results)
                        logger.info(
                            f"Пакет обработан, найдено {len(batch_results)} совпадений"
                        )

                        processed_records += len(batch_data)
                        logger.info(
                            f"Обработано {processed_records} записей из {total_records} ({(processed_records/total_records*100):.1f}%)"
                        )

                    except Exception as e:
                        logger.error(
                            f"Ошибка при обработке пакета: {str(e)}", exc_info=True
                        )
                        processed_records += BATCH_SIZE
                        continue

                logger.info(
                    f"Обработка завершена. Всего найдено {len(all_results)} совпадений"
                )

                # Сортируем результаты по дате
                all_results.sort(key=lambda x: x["dt"])
                logger.info("Результаты отсортированы по дате")

                # Автоматически заменяем номера с высокой схожестью
                high_similarity_results = [
                    r
                    for r in all_results
                    if not r.get("skipped", False)
                    and float(r["similarity"].rstrip("%")) / 100 >= 0.88
                ]

                logger.info(
                    f"Найдено {len(high_similarity_results)} номеров с высокой схожестью (>=88%)"
                )

                if high_similarity_results:
                    replaced_count = 0
                    for item in high_similarity_results:
                        try:
                            cursor.execute(
                                f"""
                                UPDATE {POTOK_TABLE}
                                SET gosnmr = %s
                                WHERE potok_id = %s
                            """,
                                (item["suggested"], item["id"]),
                            )
                            replaced_count += 1
                            if replaced_count % 10 == 0:
                                logger.info(
                                    f"Заменено {replaced_count}/{len(high_similarity_results)} номеров"
                                )
                            logger.info(
                                f"Заменен номер: {item['original']} -> {item['suggested']} (схожесть: {item['similarity']})"
                            )
                        except Exception as e:
                            logger.error(
                                f"Ошибка при замене номера: {str(e)}", exc_info=True
                            )

                    connection.commit()
                    logger.info(f"Автоматически заменено {replaced_count} номеров")

                logger.info(
                    f"Анализ завершен. Всего обработано {len(all_results)} номеров, "
                    f"из них автоматически заменено {len(high_similarity_results)}"
                )

            except Exception as e:
                logger.error(
                    f"Ошибка при работе с базой данных: {str(e)}", exc_info=True
                )

    except Exception as e:
        logger.error(
            f"Критическая ошибка в analyze_numbers_task: {str(e)}", exc_info=True
        )
