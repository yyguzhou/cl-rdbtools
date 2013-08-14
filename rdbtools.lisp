(eval-when (:compile-toplevel :load-toplevel :execute)
  (use-package :sb-ext))

(load "parser.fasl")

(defun print-rdb (dbs)
  (maphash #'(lambda (k v) (format t "KEY: ~a~%VAL: ~a~%" k v)) (nth 0 dbs)))

(defun main ()
  (if (not (eql 2 (length *posix-argv*)))
      (progn (format t "Useage: ~a redis-rdb-file~%" (first *posix-argv*))
             (exit))
      (with-open-file (s (nth 1 *posix-argv*)
                         :direction :input
                         :element-type '(unsigned-byte 8)
                         :if-does-not-exist :error)
        (print-rdb (parse-rdb s)))))

(main)