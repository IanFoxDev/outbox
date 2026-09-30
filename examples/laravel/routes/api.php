<?php

use App\Http\Controllers\OrderController;
use Illuminate\Support\Facades\Route;

Route::post('/orders', [OrderController::class, 'place']);
Route::post('/orders/{id}/pay', [OrderController::class, 'pay']);
