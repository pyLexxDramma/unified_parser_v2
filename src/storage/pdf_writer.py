from __future__ import annotations
import os
import logging
import html
from typing import Dict, Any, List, Optional
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from src.config.settings import Settings

logger = logging.getLogger(__name__)


def escape_html_for_paragraph(text: str) -> str:
    """Экранирует HTML-специальные символы для использования в Paragraph"""
    if not text:
        return ""
    # Экранируем специальные символы HTML
    text = html.escape(str(text))
    # Заменяем переносы строк на <br/>
    text = text.replace('\n', '<br/>')
    return text


class PDFWriter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.file_path = None
        self.doc = None
        self.story = []
        self.styles = getSampleStyleSheet()
        # Базовый шрифт по умолчанию (латиница)
        self.base_font_name = "Helvetica"
        # Пытаемся зарегистрировать Unicode‑шрифт, который поддерживает кириллицу
        self._register_fonts()
        self._setup_styles()

    def _register_fonts(self):
        """
        Регистрирует TrueType‑шрифт с поддержкой кириллицы для использования в PDF.
        Приоритет:
        1) переменная окружения PDF_FONT_PATH
        2) распространённые пути для DejaVu Sans / Liberation Sans / Arial
        3) если ничего не найдено — остаёмся на Helvetica
        """
        import platform
        
        # Определяем систему
        system = platform.system()
        
        font_candidates = []
        
        # Добавляем путь из переменной окружения
        env_font = os.getenv("PDF_FONT_PATH")
        if env_font:
            font_candidates.append(env_font)
        
        if system == "Windows":
            # Windows шрифты с поддержкой кириллицы
            windows_fonts_dir = os.path.join(os.environ.get('WINDIR', 'C:\\Windows'), 'Fonts')
            font_candidates.extend([
                os.path.join(windows_fonts_dir, "arial.ttf"),
                os.path.join(windows_fonts_dir, "ARIAL.TTF"),
                os.path.join(windows_fonts_dir, "arialuni.ttf"),
                os.path.join(windows_fonts_dir, "ARIALUNI.TTF"),
                os.path.join(windows_fonts_dir, "calibri.ttf"),
                os.path.join(windows_fonts_dir, "CALIBRI.TTF"),
                os.path.join(windows_fonts_dir, "times.ttf"),
                os.path.join(windows_fonts_dir, "TIMES.TTF"),
                os.path.join(windows_fonts_dir, "tahoma.ttf"),
                os.path.join(windows_fonts_dir, "TAHOMA.TTF"),
            ])
        else:
            # Linux/Mac шрифты
            font_candidates.extend([
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "/System/Library/Fonts/Helvetica.ttc",  # macOS
            ])

        for path in font_candidates:
            if not path:
                continue
            try:
                if os.path.exists(path):
                    font_name = "AppBaseFont"
                    # Регистрируем шрифт с поддержкой Unicode
                    ttf_font = TTFont(font_name, path, subfontIndex=0)
                    pdfmetrics.registerFont(ttf_font)
                    self.base_font_name = font_name
                    logger.info(f"Registered PDF font '{font_name}' from: {path}")
                    # Проверяем, что шрифт действительно зарегистрирован
                    try:
                        registered_font = pdfmetrics.getFont(font_name)
                        if registered_font:
                            logger.info(f"Font '{font_name}' successfully registered and verified")
                        else:
                            logger.warning(f"Font '{font_name}' registered but not found in pdfmetrics")
                    except Exception as check_error:
                        logger.warning(f"Could not verify font registration: {check_error}")
                    return
            except Exception as e:
                logger.warning(f"Failed to register PDF font from {path}: {e}")
                import traceback
                logger.debug(f"Traceback: {traceback.format_exc()}")
                continue

        # Если не нашли шрифт, пробуем использовать встроенный шрифт ReportLab с поддержкой кириллицы
        try:
            from reportlab.pdfbase.cidfonts import UnicodeCIDFont
            # Используем шрифт с поддержкой кириллицы из ReportLab
            # Пробуем разные варианты
            for font_family in ["Helvetica", "Times-Roman", "Courier"]:
                try:
                    pdfmetrics.registerFont(UnicodeCIDFont(font_family))
                    self.base_font_name = font_family
                    logger.info(f"Using ReportLab UnicodeCIDFont '{font_family}' for Cyrillic support")
                    return
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Failed to register UnicodeCIDFont: {e}")
        
        # Последняя попытка - используем стандартный Helvetica, но с предупреждением
        logger.warning(
            "Could not register custom TTF font or UnicodeCIDFont for PDF. "
            "Falling back to default Helvetica (русский текст может отображаться некорректно)."
        )
        logger.warning(
            "Рекомендуется установить шрифт с поддержкой кириллицы (Arial, DejaVu Sans) "
            "или установить переменную окружения PDF_FONT_PATH с путем к TTF файлу."
        )

    def _setup_styles(self):
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#1976d2'),
            spaceAfter=30,
            alignment=TA_CENTER
        ))

        self.styles.add(ParagraphStyle(
            name='CustomHeading2',
            parent=self.styles['Heading2'],
            fontSize=16,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#424242'),
            spaceAfter=12,
            spaceBefore=20
        ))

        self.styles.add(ParagraphStyle(
            name='CustomBody',
            parent=self.styles['Normal'],
            fontSize=10,
            fontName=self.base_font_name,
            spaceAfter=6
        ))

        self.styles.add(ParagraphStyle(
            name='CustomMeta',
            parent=self.styles['Normal'],
            fontSize=9,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#757575'),
            spaceAfter=3
        ))

        self.styles.add(ParagraphStyle(
            name='CustomFooter',
            parent=self.styles['Normal'],
            fontSize=8,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#666666'),
            alignment=TA_CENTER,
            spaceBefore=10
        ))

        # Стили для заголовков карточек с поддержкой кириллицы
        self.styles.add(ParagraphStyle(
            name='CustomHeading3',
            parent=self.styles['Normal'],
            fontSize=14,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#2e7d32'),
            spaceAfter=8,
            spaceBefore=12,
            fontStyle='bold'
        ))

        self.styles.add(ParagraphStyle(
            name='CustomHeading4',
            parent=self.styles['Normal'],
            fontSize=12,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#424242'),
            spaceAfter=6,
            spaceBefore=10,
            fontStyle='bold'
        ))

    def set_file_path(self, file_path: str):
        self.file_path = file_path

    def generate_report(self, output_path: str, aggregated_data: Dict[str, Any], 
                       detailed_cards: List[Dict[str, Any]], 
                       company_name: str, company_site: str) -> str:
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            self.doc = SimpleDocTemplate(
                output_path,
                pagesize=A4,
                rightMargin=2*cm,
                leftMargin=2*cm,
                topMargin=2*cm,
                bottomMargin=2*cm
            )

            self.story = []

            escaped_company_name = escape_html_for_paragraph(company_name)
            self.story.append(Paragraph(f"Отчет по компании: {escaped_company_name}", self.styles['CustomTitle']))
            self.story.append(Spacer(1, 0.5*cm))

            current_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            self.story.append(Paragraph(f"Дата создания: {current_time}", self.styles['CustomMeta']))
            if company_site:
                self.story.append(Paragraph(f"Сайт: {company_site}", self.styles['CustomMeta']))
            self.story.append(Spacer(1, 0.5*cm))

            if aggregated_data:
                self._add_statistics_section(aggregated_data)

            if detailed_cards:
                self._add_cards_section(detailed_cards)

            # Добавляем футер с авторскими правами
            self._add_footer()

            # Настраиваем футер на каждой странице
            def on_first_page(canvas, doc):
                self._draw_footer(canvas, doc)

            def on_later_pages(canvas, doc):
                self._draw_footer(canvas, doc)

            self.doc.build(self.story, onFirstPage=on_first_page, onLaterPages=on_later_pages)
            logger.info(f"PDF report generated: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error generating PDF report: {e}", exc_info=True)
            raise

    def _add_statistics_section(self, stats: Dict[str, Any]):
        self.story.append(Paragraph("Общая статистика", self.styles['CustomHeading2']))

        # Создаем стиль для ячеек таблицы
        cell_style = ParagraphStyle(
            name='TableCell',
            parent=self.styles['Normal'],
            fontSize=10,
            fontName=self.base_font_name,
            spaceAfter=0,
            spaceBefore=0
        )
        
        header_style = ParagraphStyle(
            name='TableHeader',
            parent=self.styles['Normal'],
            fontSize=12,
            fontName=self.base_font_name,
            spaceAfter=0,
            spaceBefore=0,
            textColor=colors.whitesmoke
        )

        data = [
            [Paragraph('Метрика', header_style), Paragraph('Значение', header_style)],
            [Paragraph('Количество карточек', cell_style), Paragraph(str(stats.get('total_cards_found', 0)), cell_style)],
        ]
        
        # Общий рейтинг карточки из структуры страницы (_1tam240)
        card_rating_from_page = stats.get('aggregated_card_rating_from_page', 0.0)
        if card_rating_from_page > 0:
            # Формируем читабельный текст на русском вместо звездочек
            rating_int = int(card_rating_from_page)
            rating_text = f"{rating_int} из 5 звезд"
            data.append([Paragraph('Общий рейтинг', cell_style), Paragraph(f"{card_rating_from_page:.1f} ({rating_text})", cell_style)])
        else:
            # Fallback на средний рейтинг по отзывам с текстом
            avg_rating = stats.get('aggregated_rating', 0)
            if avg_rating > 0:
                rating_int = int(avg_rating)
                rating_text = f"{rating_int} из 5 звезд"
                data.append([Paragraph('Общий рейтинг по отзывам с текстом', cell_style), Paragraph(f"{avg_rating:.2f} ({rating_text})", cell_style)])
            else:
                data.append([Paragraph('Общий рейтинг', cell_style), Paragraph("—", cell_style)])
        
        data.append([Paragraph('Всего отзывов', cell_style), Paragraph(str(stats.get('aggregated_reviews_count', 0)), cell_style)])
        
        # Всего оценок из структуры страницы (_1y88ofn или _jspzdm)
        ratings_count = stats.get('aggregated_ratings_count', 0)
        if ratings_count > 0:
            data.append([Paragraph('Всего оценок', cell_style), Paragraph(str(ratings_count), cell_style)])
        
        # Отвечено на (из структуры страницы)
        answered_count = stats.get('aggregated_answered_reviews_count', 0)
        data.append([Paragraph('Отвечено на', cell_style), Paragraph(str(answered_count), cell_style)])

        total_reviews = stats.get('aggregated_reviews_count', 0)
        answered_reviews = stats.get('aggregated_answered_reviews_count', 0)
        if total_reviews > 0:
            percent = stats.get('aggregated_answered_reviews_percent', 0) or (answered_reviews / total_reviews) * 100
            data.append([Paragraph('Процент отзывов с ответами', cell_style), Paragraph(f"{percent:.2f}%", cell_style)])
        else:
            data.append([Paragraph('Процент отзывов с ответами', cell_style), Paragraph("0%", cell_style)])

        avg_time = stats.get('aggregated_avg_response_time', 0)
        if avg_time and avg_time > 0:
            data.append([Paragraph('Среднее время ответа', cell_style), Paragraph(f"{avg_time:.2f} дней", cell_style)])
        else:
            # Если среднее время ответа недоступно, показываем процент отвеченных отзывов
            if total_reviews > 0 and answered_reviews > 0:
                percent = stats.get('aggregated_answered_reviews_percent', 0) or (answered_reviews / total_reviews) * 100
                data.append([Paragraph('Среднее время ответа', cell_style), Paragraph(f"{percent:.1f}% отвечено (дата недоступна)", cell_style)])
            else:
                data.append([Paragraph('Среднее время ответа', cell_style), Paragraph("—", cell_style)])

        data.append([Paragraph('Позитивных отзывов (4-5 звезд)', cell_style), Paragraph(str(stats.get('aggregated_positive_reviews', 0)), cell_style)])
        data.append([Paragraph('Нейтральных отзывов (3 звезды)', cell_style), Paragraph(str(stats.get('aggregated_neutral_reviews', 0)), cell_style)])
        data.append([Paragraph('Негативных отзывов (1-2 звезды)', cell_style), Paragraph(str(stats.get('aggregated_negative_reviews', 0)), cell_style)])
        
        # С оценкой (1-5 звезд) - сумма всех отзывов с рейтингом
        rated_reviews = stats.get('aggregated_rated_reviews_count', 0)
        if rated_reviews > 0:
            data.append([Paragraph('С оценкой (1-5 звезд)', cell_style), Paragraph(str(rated_reviews), cell_style)])

        table = Table(data, colWidths=[8*cm, 6*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976d2')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TOPPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))

        self.story.append(table)
        self.story.append(Spacer(1, 0.5*cm))

    def _add_cards_section(self, cards: List[Dict[str, Any]]):
        self.story.append(PageBreak())
        self.story.append(Paragraph("Детали по карточкам", self.styles['CustomHeading2']))
        self.story.append(Spacer(1, 0.3*cm))

        for idx, card in enumerate(cards, 1):
            card_name = escape_html_for_paragraph(card.get('card_name', 'Без названия'))
            self.story.append(Paragraph(f"Карточка {idx}: {card_name}", self.styles['CustomHeading3']))

            # Форматируем рейтинг карточки
            card_rating = card.get('card_rating', '—')
            if card_rating and card_rating != '—' and str(card_rating).strip():
                try:
                    rating_float = float(str(card_rating).replace(',', '.'))
                    if rating_float > 0:
                        rating_int = int(rating_float)
                        rating_text = f"{rating_float:.1f} ({rating_int} из 5 звезд)"
                    else:
                        rating_text = str(card_rating)
                except (ValueError, TypeError):
                    rating_text = str(card_rating)
            else:
                rating_text = "—"
            
            # Создаем стили для ячеек таблицы карточки
            card_cell_style = ParagraphStyle(
                name='CardTableCell',
                parent=self.styles['Normal'],
                fontSize=10,
                fontName=self.base_font_name,
                spaceAfter=0,
                spaceBefore=0
            )
            
            card_header_style = ParagraphStyle(
                name='CardTableHeader',
                parent=self.styles['Normal'],
                fontSize=10,
                fontName=self.base_font_name,
                spaceAfter=0,
                spaceBefore=0,
                textColor=colors.whitesmoke
            )
            
            card_data = [
                [Paragraph('Параметр', card_header_style), Paragraph('Значение', card_header_style)],
                [Paragraph('Адрес', card_cell_style), Paragraph(escape_html_for_paragraph(card.get('card_address', 'Не указан')), card_cell_style)],
                [Paragraph('Рейтинг', card_cell_style), Paragraph(rating_text, card_cell_style)],
                [Paragraph('Количество отзывов', card_cell_style), Paragraph(str(card.get('card_reviews_count', 0)), card_cell_style)],
            ]

            if card.get('card_answered_reviews_count') is not None:
                card_data.append([Paragraph('Отвечено отзывов', card_cell_style), Paragraph(str(card.get('card_answered_reviews_count', 0)), card_cell_style)])
                card_data.append([Paragraph('Не отвечено отзывов', card_cell_style), Paragraph(str(card.get('card_unanswered_reviews_count', 0)), card_cell_style)])

            if card.get('card_avg_response_time') and card.get('card_avg_response_time') > 0:
                card_data.append([Paragraph('Среднее время ответа', card_cell_style), Paragraph(f"{card.get('card_avg_response_time'):.2f} дней", card_cell_style)])

            card_data.append([Paragraph('Положительных отзывов', card_cell_style), Paragraph(str(card.get('card_reviews_positive', 0)), card_cell_style)])
            card_data.append([Paragraph('Отрицательных отзывов', card_cell_style), Paragraph(str(card.get('card_reviews_negative', 0)), card_cell_style)])

            if card.get('source'):
                source_name = "Яндекс.Карты" if card.get('source') == 'yandex' else "2GIS"
                card_data.append([Paragraph('Источник', card_cell_style), Paragraph(escape_html_for_paragraph(source_name), card_cell_style)])

            table = Table(card_data, colWidths=[6*cm, 8*cm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                ('TOPPADDING', (0, 1), (-1, -1), 6),
                ('LEFTPADDING', (0, 0), (-1, -1), 6),
                ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ]))

            self.story.append(table)
            self.story.append(Spacer(1, 0.3*cm))

            reviews = card.get('detailed_reviews', [])
            if reviews:
                self.story.append(Paragraph("Отзывы:", self.styles['CustomHeading4']))
                for review_idx, review in enumerate(reviews[:10], 1):
                    review_text = f"<b>Отзыв {review_idx}</b><br/>"
                    if review.get('review_author'):
                        review_text += f"Автор: {escape_html_for_paragraph(review.get('review_author'))}<br/>"
                    if review.get('review_rating'):
                        rating_value = review.get('review_rating', 0)
                        try:
                            rating_float = float(str(rating_value).replace(',', '.'))
                            if rating_float > 0:
                                rating_int = int(rating_float)
                                # Используем читабельный текст на русском вместо звездочек
                                # Формат: "5.0 (5 из 5 звезд)" - полностью читабельно
                                rating_text = f"{rating_int} из 5 звезд"
                                review_text += f"Рейтинг: {rating_float:.1f} ({rating_text})<br/>"
                            else:
                                review_text += f"Рейтинг: не указан<br/>"
                        except (ValueError, TypeError):
                            # Если не удалось преобразовать в число, показываем как есть
                            review_text += f"Рейтинг: {escape_html_for_paragraph(str(rating_value))}<br/>"
                    elif review.get('review_text'):  # Если нет рейтинга, но есть текст, показываем "Рейтинг не указан"
                        review_text += f"Рейтинг: не указан<br/>"
                    if review.get('review_date'):
                        review_text += f"Дата: {escape_html_for_paragraph(str(review.get('review_date')))}<br/>"
                    if review.get('review_text'):
                        # Показываем полный текст отзыва без обрезки, экранируем HTML
                        review_text += f"Текст: {escape_html_for_paragraph(review.get('review_text'))}"

                    self.story.append(Paragraph(review_text, self.styles['CustomBody']))
                    
                    # Добавляем ответ организации, если он есть
                    if review.get('has_response') and review.get('response_text'):
                        response_text = f"<b>Ответ организации:</b><br/>"
                        if review.get('response_date'):
                            response_text += f"Дата ответа: {escape_html_for_paragraph(str(review.get('response_date')))}<br/>"
                        response_text += escape_html_for_paragraph(review.get('response_text'))
                        self.story.append(Paragraph(response_text, self.styles['CustomBody']))
                    
                    self.story.append(Spacer(1, 0.2*cm))

                if len(reviews) > 10:
                    self.story.append(Paragraph(f"... и еще {len(reviews) - 10} отзывов", self.styles['CustomMeta']))

            self.story.append(Spacer(1, 0.5*cm))

            if idx % 2 == 0 and idx < len(cards):
                self.story.append(PageBreak())

    def _add_footer(self):
        """Добавляет футер с авторскими правами в конец документа"""
        self.story.append(Spacer(1, 1*cm))
        footer_text = (
            f"© {datetime.now().year} Разработано: "
            f"<link href='https://github.com/pyLexxDramma' color='blue'>"
            f"<u>GitHub: pyLexxDramma</u></link>"
        )
        self.story.append(Paragraph(footer_text, self.styles['CustomFooter']))

    def _draw_footer(self, canvas, doc):
        """Рисует футер на каждой странице PDF"""
        canvas.saveState()
        
        # Текст футера с HTML для правильного отображения кириллицы
        footer_text = f"© {datetime.now().year} Разработано: <link href='https://github.com/pyLexxDramma' color='blue'><u>GitHub: pyLexxDramma</u></link>"
        github_url = "https://github.com/pyLexxDramma"
        
        # Используем Paragraph для правильного отображения Unicode текста
        footer_style = ParagraphStyle(
            name='FooterStyle',
            parent=self.styles['Normal'],
            fontSize=8,
            fontName=self.base_font_name,
            textColor=colors.HexColor('#666666'),
            alignment=TA_CENTER,
            spaceAfter=0,
            spaceBefore=0
        )
        
        footer_paragraph = Paragraph(footer_text, footer_style)
        
        # Позиция футера (внизу страницы)
        footer_y = 1*cm
        page_width = doc.pagesize[0]
        page_height = doc.pagesize[1]
        
        # Вычисляем ширину и высоту параграфа
        footer_paragraph.wrapOn(canvas, page_width - 2*cm, page_height)
        footer_height = footer_paragraph.height
        
        # Центрируем по горизонтали
        x = doc.leftMargin
        
        # Рисуем параграф
        footer_paragraph.drawOn(canvas, x, footer_y)
        
        canvas.restoreState()


