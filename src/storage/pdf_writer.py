from __future__ import annotations
import os
import logging
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
        font_candidates = [
            os.getenv("PDF_FONT_PATH"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "C:/Windows/Fonts/arial.ttf",
            "C:/Windows/Fonts/ARIAL.TTF",
        ]

        for path in font_candidates:
            if not path:
                continue
            try:
                if os.path.exists(path):
                    font_name = "AppBaseFont"
                    pdfmetrics.registerFont(TTFont(font_name, path))
                    self.base_font_name = font_name
                    logger.info(f"Registered PDF font '{font_name}' from: {path}")
                    return
            except Exception as e:
                logger.warning(f"Failed to register PDF font from {path}: {e}")

        logger.warning(
            "Could not register custom TTF font for PDF. "
            "Falling back to default Helvetica (русский текст может отображаться некорректно)."
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

            self.story.append(Paragraph(f"Отчет по компании: {company_name}", self.styles['CustomTitle']))
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

            self.doc.build(self.story)
            logger.info(f"PDF report generated: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error generating PDF report: {e}", exc_info=True)
            raise

    def _add_statistics_section(self, stats: Dict[str, Any]):
        self.story.append(Paragraph("Общая статистика", self.styles['CustomHeading2']))

        data = [
            ['Метрика', 'Значение'],
            ['Карточек найдено', str(stats.get('total_cards_found', 0))],
            ['Средний рейтинг', f"{stats.get('aggregated_rating', 0):.2f}" if stats.get('aggregated_rating') else "—"],
            ['Всего отзывов', str(stats.get('aggregated_reviews_count', 0))],
            ['Отвечено отзывов', str(stats.get('aggregated_answered_reviews_count', 0))],
        ]

        total_reviews = stats.get('aggregated_reviews_count', 0)
        answered_reviews = stats.get('aggregated_answered_reviews_count', 0)
        if total_reviews > 0:
            percent = stats.get('aggregated_answered_reviews_percent', 0) or (answered_reviews / total_reviews) * 100
            data.append(['Процент отзывов с ответами', f"{percent:.2f}%"])
        else:
            data.append(['Процент отзывов с ответами', "0%"])

        avg_time = stats.get('aggregated_avg_response_time', 0)
        if avg_time:
            data.append(['Среднее время ответа', f"{avg_time:.2f} дней"])
        else:
            data.append(['Среднее время ответа', "—"])

        data.append(['Положительных отзывов (4-5⭐)', str(stats.get('aggregated_positive_reviews', 0))])
        data.append(['Отрицательных отзывов (1-3⭐)', str(stats.get('aggregated_negative_reviews', 0))])

        table = Table(data, colWidths=[8*cm, 6*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976d2')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), self.base_font_name),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))

        self.story.append(table)
        self.story.append(Spacer(1, 0.5*cm))

    def _add_cards_section(self, cards: List[Dict[str, Any]]):
        self.story.append(PageBreak())
        self.story.append(Paragraph("Детали по карточкам", self.styles['CustomHeading2']))
        self.story.append(Spacer(1, 0.3*cm))

        for idx, card in enumerate(cards, 1):
            self.story.append(Paragraph(f"Карточка {idx}: {card.get('card_name', 'Без названия')}", self.styles['Heading3']))

            card_data = [
                ['Параметр', 'Значение'],
                ['Адрес', card.get('card_address', 'Не указан')],
                ['Рейтинг', str(card.get('card_rating', '—'))],
                ['Количество отзывов', str(card.get('card_reviews_count', 0))],
            ]

            if card.get('card_answered_reviews_count') is not None:
                card_data.append(['Отвечено отзывов', str(card.get('card_answered_reviews_count', 0))])
                card_data.append(['Не отвечено отзывов', str(card.get('card_unanswered_reviews_count', 0))])

            if card.get('card_avg_response_time'):
                card_data.append(['Среднее время ответа', f"{card.get('card_avg_response_time')} дней"])

            card_data.append(['Положительных отзывов', str(card.get('card_reviews_positive', 0))])
            card_data.append(['Отрицательных отзывов', str(card.get('card_reviews_negative', 0))])

            if card.get('source'):
                source_name = "Яндекс.Карты" if card.get('source') == 'yandex' else "2GIS"
                card_data.append(['Источник', source_name])

            table = Table(card_data, colWidths=[6*cm, 8*cm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), self.base_font_name),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))

            self.story.append(table)
            self.story.append(Spacer(1, 0.3*cm))

            reviews = card.get('detailed_reviews', [])
            if reviews:
                self.story.append(Paragraph("Отзывы:", self.styles['Heading4']))
                for review_idx, review in enumerate(reviews[:10], 1):
                    review_text = f"<b>Отзыв {review_idx}</b><br/>"
                    if review.get('review_author'):
                        review_text += f"Автор: {review.get('review_author')}<br/>"
                    if review.get('review_rating'):
                        review_text += f"Рейтинг: {'⭐' * int(review.get('review_rating', 0))} ({review.get('review_rating')})<br/>"
                    elif review.get('review_text'):  # Если нет рейтинга, но есть текст, показываем "Рейтинг не указан"
                        review_text += f"Рейтинг: не указан<br/>"
                    if review.get('review_date'):
                        review_text += f"Дата: {review.get('review_date')}<br/>"
                    if review.get('review_text'):
                        # Показываем полный текст отзыва без обрезки
                        review_text += f"Текст: {review.get('review_text')}"

                    self.story.append(Paragraph(review_text, self.styles['CustomBody']))
                    
                    # Добавляем ответ организации, если он есть
                    if review.get('has_response') and review.get('response_text'):
                        response_text = f"<b>Ответ организации:</b><br/>"
                        if review.get('response_date'):
                            response_text += f"Дата ответа: {review.get('response_date')}<br/>"
                        response_text += f"{review.get('response_text')}"
                        self.story.append(Paragraph(response_text, self.styles['CustomBody']))
                    
                    self.story.append(Spacer(1, 0.2*cm))

                if len(reviews) > 10:
                    self.story.append(Paragraph(f"... и еще {len(reviews) - 10} отзывов", self.styles['CustomMeta']))

            self.story.append(Spacer(1, 0.5*cm))

            if idx % 2 == 0 and idx < len(cards):
                self.story.append(PageBreak())


