import numpy as np
import matplotlib.pyplot as plt
from math import cos, sin, radians
from scipy.signal import find_peaks

def generate_turn_trajectory():
    # Параметры траектории
    total_time = 30.0  # секунд
    straight_distance = 15.0  # метров до разворота
    turn_angle = 170  # градусов
    
    # Время для разворота (1 секунда)
    turn_time = 1.0
    
    # Время для прямолинейного движения до разворота
    first_straight_time = (total_time - turn_time) / 2
    
    # Время для прямолинейного движения после разворота
    second_straight_time = (total_time - turn_time) / 2
    
    # Рассчитываем скорость
    speed = straight_distance / first_straight_time  # м/с
    
    # Увеличиваем частоту дискретизации для плавного разворота
    dt = 0.02  # дискретизация 50 Гц
    time_points = np.arange(0, total_time + dt, dt)
    
    # Массивы для координат
    x_points = []
    y_points = []
    
    # Начальное направление (вдоль оси X)
    current_angle = 0  # радиан
    
    for t in time_points:
        if t <= first_straight_time:
            # Первое прямолинейное движение
            distance = speed * t
            x = distance * cos(current_angle)
            y = distance * sin(current_angle)
            
        elif t <= first_straight_time + turn_time:
            # Мгновенный разворот
            # Достигаем точки разворота
            x = straight_distance * cos(current_angle)
            y = straight_distance * sin(current_angle)
            
            # Вычисляем угол поворота (плавный за 1 секунду)
            turn_progress = (t - first_straight_time) / turn_time
            turn_angle_rad = radians(turn_angle) * turn_progress
            
        else:
            # Второе прямолинейное движение после поворота
            # Угол после поворота
            final_angle = radians(turn_angle)
            
            # Время движения после поворота
            move_time = t - (first_straight_time + turn_time)
            
            # Начальная точка для второго участка - точка разворота
            start_x = straight_distance * cos(current_angle)
            start_y = straight_distance * sin(current_angle)
            
            # Движение в новом направлении
            distance_after_turn = speed * move_time
            
            x = start_x + distance_after_turn * cos(final_angle)
            y = start_y + distance_after_turn * sin(final_angle)
        
        x_points.append(x)
        y_points.append(y)
    
    return time_points, np.array(x_points), np.array(y_points)

def find_peaks_and_valleys(x_data, y_data, height=None, prominence=None, distance=None):

    peaks_x, _ = find_peaks(x_data, height=height, prominence=prominence, distance=distance)
    peaks_y, _ = find_peaks(y_data, height=height, prominence=prominence, distance=distance)
    
    # Находим впадины для x и y (пики инвертированного сигнала)
    valleys_x, _ = find_peaks(-x_data, height=height, prominence=prominence, distance=distance)
    valleys_y, _ = find_peaks(-y_data, height=height, prominence=prominence, distance=distance)
    
    return {
        'peaks_x': peaks_x,
        'valleys_x': valleys_x,
        'peaks_y': peaks_y,
        'valleys_y': valleys_y
    }

# Генерируем траекторию
time, x, y = generate_turn_trajectory()

# Находим пики и впадины
peaks_data = find_peaks_and_valleys(x, y, height=0.1, distance=100)

# Выводим координаты в консоль
print("Координаты траектории (x, y):")
print("Время(s)\tX(m)\t\tY(m)")
for i in range(0, len(time), 50):  # Выводим каждую 50-ю точку для читаемости
    print(f"{time[i]:.2f}\t\t{x[i]:.3f}\t\t{y[i]:.3f}")

# Находим индексы ключевых точек
turn_start_idx = np.argmax(time >= 14.5)  # начало разворота
turn_end_idx = np.argmax(time >= 15.5)    # конец разворота

print(f"\nВсего точек: {len(x)}")
print(f"Частота дискретизации: {1/(time[1]-time[0]):.1f} Гц")
print(f"Расстояние до разворота: {np.sqrt(x[turn_start_idx]**2 + y[turn_start_idx]**2):.3f} м")

# Выводим информацию о найденных пиках и впадинах
print(f"\n=== НАЙДЕННЫЕ ПИКИ И ВПАДИНЫ ===")
print("Пики по X координате:")
for idx in peaks_data['peaks_x']:
    print(f"  Время: {time[idx]:.2f} с, X: {x[idx]:.3f} м, Y: {y[idx]:.3f} м")

print("\nВпадины по X координате:")
for idx in peaks_data['valleys_x']:
    print(f"  Время: {time[idx]:.2f} с, X: {x[idx]:.3f} м, Y: {y[idx]:.3f} м")

print("\nПики по Y координате:")
for idx in peaks_data['peaks_y']:
    print(f"  Время: {time[idx]:.2f} с, X: {x[idx]:.3f} м, Y: {y[idx]:.3f} м")

print("\nВпадины по Y координате:")
for idx in peaks_data['valleys_y']:
    print(f"  Время: {time[idx]:.2f} с, X: {x[idx]:.3f} м, Y: {y[idx]:.3f} м")

# Строим визуализацию
plt.figure(figsize=(16, 12))

# Траектория с пиками и впадинами
plt.subplot(2, 2, 1)
plt.plot(x, y, 'b-', linewidth=2, label='Траектория', alpha=0.7)
plt.plot(x[0], y[0], 'go', markersize=10, label='Старт')
plt.plot(x[turn_start_idx], y[turn_start_idx], 'ro', markersize=8, label='Начало поворота')
plt.plot(x[turn_end_idx], y[turn_end_idx], 'yo', markersize=8, label='Конец поворота')
plt.plot(x[-1], y[-1], 'mo', markersize=10, label='Финиш')

# Отмечаем пики и впадины
plt.plot(x[peaks_data['peaks_x']], y[peaks_data['peaks_x']], 'r^', markersize=8, label='Пики X', alpha=0.8)
plt.plot(x[peaks_data['valleys_x']], y[peaks_data['valleys_x']], 'rv', markersize=8, label='Впадины X', alpha=0.8)
plt.plot(x[peaks_data['peaks_y']], y[peaks_data['peaks_y']], 'b^', markersize=8, label='Пики Y', alpha=0.8)
plt.plot(x[peaks_data['valleys_y']], y[peaks_data['valleys_y']], 'bv', markersize=8, label='Впадины Y', alpha=0.8)

# Добавляем стрелки для направления
arrow1_idx = len(x) // 6
plt.arrow(x[arrow1_idx], y[arrow1_idx], 
          x[arrow1_idx+5]-x[arrow1_idx], y[arrow1_idx+5]-y[arrow1_idx],
          head_width=0.3, head_length=0.5, fc='green', ec='green')

arrow2_idx = 5 * len(x) // 6
plt.arrow(x[arrow2_idx], y[arrow2_idx], 
          x[arrow2_idx+5]-x[arrow2_idx], y[arrow2_idx+5]-y[arrow2_idx],
          head_width=0.3, head_length=0.5, fc='red', ec='red')

plt.xlabel('X (м)')
plt.ylabel('Y (м)')
plt.title('Траектория движения с мгновенным разворотом на 170°\n(пики и впадины)')
plt.grid(True)
plt.axis('equal')
plt.legend()

# Координаты по времени с пиками и впадинами
plt.subplot(2, 2, 2)
plt.plot(time, x, 'r-', label='X координата', alpha=0.7)
plt.plot(time, y, 'b-', label='Y координата', alpha=0.7)

# Отмечаем пики и впадины на графиках координат
plt.plot(time[peaks_data['peaks_x']], x[peaks_data['peaks_x']], 'r^', markersize=6, label='Пики X')
plt.plot(time[peaks_data['valleys_x']], x[peaks_data['valleys_x']], 'rv', markersize=6, label='Впадины X')
plt.plot(time[peaks_data['peaks_y']], y[peaks_data['peaks_y']], 'b^', markersize=6, label='Пики Y')
plt.plot(time[peaks_data['valleys_y']], y[peaks_data['valleys_y']], 'bv', markersize=6, label='Впадины Y')

# Вертикальные линии для разделения сегментов
plt.axvline(x=time[turn_start_idx], color='gray', linestyle='--', alpha=0.7, label='Начало поворота')
plt.axvline(x=time[turn_end_idx], color='gray', linestyle=':', alpha=0.7, label='Конец поворота')

plt.xlabel('Время (с)')
plt.ylabel('Координата (м)')
plt.title('Координаты по времени с пиками и впадинами')
plt.grid(True)
plt.legend()

# Скорость
plt.subplot(2, 2, 3)
dx = np.diff(x)
dy = np.diff(y)
velocity = np.sqrt(dx**2 + dy**2) / np.diff(time)
plt.plot(time[:-1], velocity, 'g-', linewidth=2, alpha=0.7)

# Вертикальные линии для разделения сегментов
plt.axvline(x=time[turn_start_idx], color='gray', linestyle='--', alpha=0.7)
plt.axvline(x=time[turn_end_idx], color='gray', linestyle=':', alpha=0.7)

plt.xlabel('Время (с)')
plt.ylabel('Скорость (м/с)')
plt.title('Скорость движения')
plt.grid(True)

# Направление
plt.subplot(2, 2, 4)
direction = np.degrees(np.arctan2(dy, dx))
# Нормализуем направление к диапазону [0, 360)
direction = np.mod(direction, 360)
plt.plot(time[:-1], direction, 'purple', linewidth=2, alpha=0.7)

# Вертикальные линии для разделения сегментов
plt.axvline(x=time[turn_start_idx], color='gray', linestyle='--', alpha=0.7)
plt.axvline(x=time[turn_end_idx], color='gray', linestyle=':', alpha=0.7)

plt.xlabel('Время (с)')
plt.ylabel('Направление (°)')
plt.title('Направление движения')
plt.grid(True)

plt.tight_layout()
plt.savefig('instant_turn_trajectory_with_peaks.png', dpi=300, bbox_inches='tight')
plt.show()

# Дополнительная информация
print(f"\n=== ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ ===")
print(f"Начальная позиция: ({x[0]:.3f}, {y[0]:.3f}) м")
print(f"Точка разворота: ({x[turn_start_idx]:.3f}, {y[turn_start_idx]:.3f}) м")
print(f"Конечная позиция: ({x[-1]:.3f}, {y[-1]:.3f}) м")
print(f"Расстояние до разворота: {np.sqrt(x[turn_start_idx]**2 + y[turn_start_idx]**2):.3f} м")
print(f"Расстояние после разворота: {np.sqrt((x[-1]-x[turn_end_idx])**2 + (y[-1]-y[turn_end_idx])**2):.3f} м")
print(f"Общее пройденное расстояние: {np.sum(np.sqrt(np.diff(x)**2 + np.diff(y)**2)):.3f} м")
print(f"Скорость движения: {speed:.3f} м/с")
print(f"Время разворота: {turn_time:.1f} с")
print(f"Время первого участка: {first_straight_time:.1f} с")
print(f"Время второго участка: {second_straight_time:.1f} с")