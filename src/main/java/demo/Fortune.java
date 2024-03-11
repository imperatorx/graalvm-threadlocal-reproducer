/*
 * Copyright (c) 2023, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * The Universal Permissive License (UPL), Version 1.0
 *
 * Subject to the condition set forth below, permission is hereby granted to any
 * person obtaining a copy of this software, associated documentation and/or
 * data (collectively the "Software"), free of charge and under any and all
 * copyright rights in the Software, and any and all patent rights owned or
 * freely licensable by each licensor hereunder covering either (i) the
 * unmodified Software as contributed to or provided by such licensor, or (ii)
 * the Larger Works (as defined below), to deal in both
 *
 * (a) the Software, and
 *
 * (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
 * one is included with the Software each a "Larger Work" to which the Software
 * is contributed by such licensors),
 *
 * without restriction, including without limitation the rights to copy, create
 * derivative works of, display, perform, and distribute the Software and make,
 * use, sell, offer for sale, import, export, have made, and have sold the
 * Software and the Larger Work(s), and to sublicense the foregoing rights on
 * either these or other terms.
 *
 * This license is subject to the following condition:
 *
 * The above copyright notice and either this complete permission notice or at a
 * minimum a reference to the UPL must be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package demo;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Reproducer:
 * <p>
 * Use linux.
 * Install https://github.com/apple/foundationdb/releases/download/7.3.27/foundationdb-clients-7.3.27-1.el7.x86_64.rpm
 * Install https://github.com/apple/foundationdb/releases/download/7.3.27/foundationdb-server-7.3.27-1.el7.x86_64.rpm
 * Build native executable
 * Run native executable in an infinite loop
 * observe error stack trace after minutes
 */
public class Fortune {

    private static final ThreadLocal<Object> THREAD_LOCAL = new ThreadLocal<>();

    public static void main(String[] args) throws Exception {

        var factory = Thread.ofVirtual()
                .factory();

        var executor = Executors.newThreadPerTaskExecutor(factory);
        var fdb = FDB.selectAPIVersion(730);

        try {
            try (var db = fdb.open(null, Runnable::run)) {
                for (int i = 0; i < 10; i++) {
                    background(db, i, executor);
                }
                Thread.sleep(3000);
            }
        } finally {
            fdb.stopNetwork();
        }
    }

    private static void background(Database db, int i, Executor executor) {
        executor.execute(() -> {
            while (true) {
                try {
                    loop(db, i);
                } catch (Exception e) {
                    // Ignore dirty close
                    if (!e.getMessage().contains("Cannot access closed object")) {
                        e.printStackTrace();
                        e.getCause().printStackTrace();
                    }
                }
            }
        });
    }

    private static void loop(Database db, int i) {

        Object scope = createScope();
        var tr = db.createTransaction(Runnable::run);

        try {
            for (int j = 0; j < 100; j++) {
                int finalJ = j;
                forkScope(scope, () -> doWork(tr, finalJ));
            }
            joinScope(scope);
        } finally {
            closeScope(scope);
            tr.close();
        }
    }

    private static int doWork(Transaction tr, int j) {

        THREAD_LOCAL.get();
        THREAD_LOCAL.set(new Object());

        tr.get(("foo" + j).getBytes())
                .join();
        return 1;
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Reflection magic, so we don't have to turn on --enable-preview for the maven exec plugin :)
    /////////////////////////////////////////////////////////////////////////////////////////////////////////


    private final static MethodHandle scopeConstructorHandle = crateScopeConstructorHandle();

    private final static MethodHandle scopeJoinHandle = createJoinHandle();

    private final static MethodHandle scopeThrowIfFailedHandle = createThrowIfFailedHandle();

    private final static MethodHandle scopeCloseHandle = createCloseHandle();

    private final static MethodHandle scopeForkHandle = createForkHandle();

    private static Class taskScopeClass() {
        try {
            return Class.forName("java.util.concurrent.StructuredTaskScope$ShutdownOnFailure");
        } catch (Throwable e) {
            throw new RuntimeException("Structured concurrency is not supported.", e);
        }
    }

    private static MethodHandle crateScopeConstructorHandle() {

        MethodHandles.Lookup lookup = MethodHandles.lookup();

        try {
            Class clazz = taskScopeClass();
            return lookup.findConstructor(clazz, MethodType.methodType(void.class));
        } catch (Throwable e) {
            throw new RuntimeException("Structured concurrency is not supported.", e);
        }
    }

    private static MethodHandle createJoinHandle() {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class clazz = taskScopeClass();
            return lookup.findVirtual(clazz, "join", MethodType.methodType(clazz));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static MethodHandle createThrowIfFailedHandle() {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class clazz = taskScopeClass();
            return lookup.findVirtual(clazz, "throwIfFailed", MethodType.methodType(void.class));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static MethodHandle createCloseHandle() {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class clazz = taskScopeClass();
            return lookup.findVirtual(clazz, "close", MethodType.methodType(void.class));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static MethodHandle createForkHandle() {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class clazz = taskScopeClass();
            Class handleClazz = Class.forName("java.util.concurrent.StructuredTaskScope$Subtask");
            return lookup.findVirtual(clazz, "fork", MethodType.methodType(handleClazz, Callable.class));
        } catch (Throwable e) {
            throw new RuntimeException("Unable to create JDK 21+ scope fork handle", e);
        }
    }


    private static Object createScope() {
        try {
            return scopeConstructorHandle.invoke();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeScope(Object scope) {
        try {
            scopeCloseHandle.invoke(scope);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Supplier<T> forkScope(Object scope, Callable<T> task) {
        try {
            return (Supplier<T>) scopeForkHandle.invoke(scope, task);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void joinScope(Object scope) {
        try {
            scopeJoinHandle.invoke(scope);
            scopeThrowIfFailedHandle.invoke(scope);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
